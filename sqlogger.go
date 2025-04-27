package sqlogger

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	_ "github.com/mattn/go-sqlite3"
)

const defaultMaxSizeLiveLog = 50000
const defaultNumLogFiles = 7

const logFileBasename = "logs"
const logFileExtension = "sqlite"

const openLogSQL = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;

DROP TABLE IF EXISTS entries;

CREATE TABLE IF NOT EXISTS entries (
  epoch_secs LONG,
  nanos INTEGER, 
  level INTEGER,  
  content BLOB
);
`
const resetLogSQL = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;

DROP TABLE IF EXISTS entries;

CREATE TABLE IF NOT EXISTS entries (
  epoch_secs LONG,
  nanos INTEGER, 
  level INTEGER,  
  content BLOB
);
`

// groupOrAttrs holds either a group name or a list of slog.Attrs.
type groupOrAttrs struct {
	group string      // group name if non-empty
	attrs []slog.Attr // attrs if non-empty
}

type SQLogger struct {
	opts         Options
	goas         []groupOrAttrs
	currentName  string
	currentLogId int
	db           *sql.DB
	lastInsertId int64
	stdHandler   slog.Handler
	cwd          string
}

type Options struct {
	// Level reports the minimum level to log.
	// Levels with lower levels are discarded.
	// If nil, the Handler uses [slog.LevelInfo].
	Level slog.Leveler

	// The maximum number of log entries per database file
	maxSizeLiveLog int

	// The number of database files for log rotation
	numLogFiles int

	// Set to true to disable color output to console
	NoColor bool
}

func NewSQLogger(opts *Options) (*SQLogger, error) {

	h := &SQLogger{}

	if opts != nil {
		h.opts = *opts
	}

	// Check for defaults if the user did not specify values
	if h.opts.Level == nil {
		h.opts.Level = slog.LevelInfo
	}
	if h.opts.maxSizeLiveLog == 0 {
		h.opts.maxSizeLiveLog = defaultMaxSizeLiveLog
	}
	if h.opts.numLogFiles == 0 {
		h.opts.numLogFiles = defaultNumLogFiles
	}

	// Enable or disable colored output to console
	color.NoColor = h.opts.NoColor

	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("failed to get the working directory: %w", err)
	}
	h.cwd = cwd

	h.stdHandler = slog.Default().Handler()

	// Determine the current database being used from the possible many in the rotation
	currentName, err := DetermineCurrentName()
	if err != nil {
		return nil, err
	}
	h.currentName = currentName

	db, err := sql.Open("sqlite3", h.currentName)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(openLogSQL)
	if err != nil {
		return nil, err
	}

	h.db = db

	return h, nil

}

func DetermineCurrentName() (string, error) {

	// Read all entries in the current directory
	dirEntry, err := os.ReadDir(".")
	if err != nil {
		return "", err
	}

	var candidateFileName string
	candidateLogNumber := 0
	minimumModificationTime := int64(0)

	for _, entry := range dirEntry {
		// Skip entries which are directories and handle only files
		if entry.IsDir() {
			continue
		}

		// Skip files with a name not according to the pattern name.aNumber.extension
		parts := strings.Split(entry.Name(), ".")
		if len(parts) != 3 {
			continue
		}

		// Skip files without the exact name and extension
		if parts[0] != logFileBasename || parts[2] != logFileExtension {
			continue
		}

		// We found a log file, check its modification time agains the current minimum
		info, err := entry.Info()
		if err != nil {
			return "", err
		}

		if info.ModTime().Unix() < minimumModificationTime {
			continue
		}

		// We will account for the very strange case where two log files have the same modification time
		// We will choose the one with greater log number or when the current entry number is 0 and
		// the candidate is numLogFiles-1

		entryLogNumber, err := strconv.Atoi(parts[1])
		if err != nil {
			return "", err
		}

		if (entryLogNumber > candidateLogNumber) || (entryLogNumber == 0 && candidateLogNumber == defaultNumLogFiles-1) {
			minimumModificationTime = info.ModTime().Unix()
			candidateFileName = entry.Name()
			candidateLogNumber = entryLogNumber
		}

	}

	// If we are starting the first time, we would not find any files complying with the naming
	if candidateFileName == "" {
		return fmt.Sprintf("%s.%d.%s", logFileBasename, 0, logFileExtension), nil
	} else {
		return candidateFileName, nil
	}
}

func (h *SQLogger) Rotate() error {
	// Close the current log database
	h.db.Close()

	// Increment the log ID
	h.currentLogId++
	if h.currentLogId >= defaultNumLogFiles {
		h.currentLogId = 0
	}

	// Get the next file name
	h.currentName = fmt.Sprintf("%s.%d.%s", logFileBasename, h.currentLogId, logFileExtension)
	slog.Info("rotating log file", "name", h.currentLogId)

	// Open the new log database
	db, err := sql.Open("sqlite3", h.currentName)
	if err != nil {
		return err
	}

	// Create the table
	_, err = db.Exec(resetLogSQL)
	if err != nil {
		return err
	}

	h.db = db

	return nil
}

func (h *SQLogger) Name() string {
	return "SQLogger"
}

func (h *SQLogger) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.opts.Level.Level()
}

func (h *SQLogger) Handle(c context.Context, r slog.Record) error {

	// Get a byte buffer from the pool and defer returning it to the pool
	bufp := allocBuf()
	bufColor := *bufp
	defer func() {
		*bufp = bufColor
		freeBuf(bufp)
	}()

	bufp2 := allocBuf()
	bufPlain := *bufp2
	defer func() {
		*bufp2 = bufPlain
		freeBuf(bufp2)
	}()

	// We do not follow the usual rule for handlers of ignoring empty timestamp
	// We need the timestamp for the database
	if r.Time.IsZero() {
		r.Time = time.Now()
	}

	const glevel = 130
	greyColor := color.RGB(glevel, glevel, glevel)

	// The string representation of the log time
	logTime := r.Time.Format(time.TimeOnly)
	logTimeColored := greyColor.Sprint(logTime)

	// Color the level and set minimum length of 5 chars
	level := r.Level.String()

	coloredLevel := level
	switch r.Level {
	case slog.LevelDebug:
		coloredLevel = color.MagentaString(level)
	case slog.LevelInfo:
		coloredLevel = color.GreenString(level)
	case slog.LevelWarn:
		coloredLevel = color.YellowString(level)
	case slog.LevelError:
		coloredLevel = color.RedString(level)
	}

	var undecoratedLocation string
	var decoratedLocation string

	// The location of the log call
	if r.PC != 0 {
		fs := runtime.CallersFrames([]uintptr{r.PC})
		f, _ := fs.Next()

		dir, file := filepath.Split(f.File)

		// Trim the root directory prefix to get the relative directory of the source file
		var fullFileName string
		relativeDir, err := filepath.Rel(h.cwd, filepath.Dir(dir))
		if err != nil {
			fullFileName = f.File
		} else {
			fullFileName = filepath.Join(relativeDir, file)
		}

		undecoratedLocation = fmt.Sprintf("%s:%d", fullFileName, f.Line)
		decoratedLocation = color.BlueString(undecoratedLocation)

	}

	// *******************************************
	// timestamp
	// *******************************************
	bufColor = append(bufColor, logTimeColored...)
	bufColor = append(bufColor, ' ')

	bufPlain = append(bufPlain, logTime...)
	bufPlain = append(bufPlain, ' ')

	// *******************************************
	// level
	// *******************************************
	bufColor = append(bufColor, coloredLevel...)
	bufColor = append(bufColor, ' ')
	if len(level) < 5 {
		bufColor = append(bufColor, ' ')
	}

	bufPlain = append(bufPlain, level...)
	bufPlain = append(bufPlain, ' ')
	if len(level) < 5 {
		bufPlain = append(bufPlain, ' ')
	}

	// *******************************************
	// location
	// *******************************************

	bufColor = append(bufColor, decoratedLocation...)
	bufColor = append(bufColor, ' ')

	bufPlain = append(bufPlain, undecoratedLocation...)
	bufPlain = append(bufPlain, ' ')

	// *******************************************
	// message
	// *******************************************

	bufColor = append(bufColor, r.Message...)
	bufColor = append(bufColor, ' ')

	bufPlain = append(bufPlain, r.Message...)
	bufPlain = append(bufPlain, ' ')

	// *******************************************
	// *******************************************

	// Handle state from WithGroup and WithAttrs.
	goas := h.goas
	if r.NumAttrs() == 0 {
		// If the record has no Attrs, remove groups at the end of the list; they are empty.
		for len(goas) > 0 && goas[len(goas)-1].group != "" {
			goas = goas[:len(goas)-1]
		}
	}

	for _, goa := range goas {
		if goa.group != "" {
			bufColor = fmt.Appendf(bufColor, "%s ", goa.group)
		} else {
			for _, a := range goa.attrs {
				bufColor = h.appendAttr(bufColor, a, greyColor)
			}
		}
	}

	r.Attrs(func(a slog.Attr) bool {
		bufColor = h.appendAttr(bufColor, a, greyColor)
		return true
	})

	bufColor = append(bufColor, '\n')
	bufPlain = append(bufPlain, '\n')

	// Print the colored buffer to standard output as a normal log
	// fmt.Println(string(bufColor))
	os.Stdout.Write(bufColor)

	// Insert the undecorated buffer into the log database
	stmt, err := h.db.Prepare("insert into entries (epoch_secs, nanos, level, content) values(?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	result, err := stmt.Exec(r.Time.Unix(), r.Time.Nanosecond(), r.Level, string(bufPlain))
	if err != nil {
		return fmt.Errorf("inserting log record: %w", err)
	}

	// Check if the current log file has reached the maximum number of entries, and rotate the log if so
	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("retrieving last insert id: %w", err)
	}
	h.lastInsertId = id

	if h.lastInsertId >= defaultMaxSizeLiveLog {
		h.Rotate()
	}

	return nil
}

func (h *SQLogger) withGroupOrAttrs(goa groupOrAttrs) *SQLogger {
	h2 := *h
	h2.goas = make([]groupOrAttrs, len(h.goas)+1)
	copy(h2.goas, h.goas)
	h2.goas[len(h2.goas)-1] = goa
	return &h2
}

func (h *SQLogger) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	return h.withGroupOrAttrs(groupOrAttrs{attrs: attrs})
}

func (h *SQLogger) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return h.withGroupOrAttrs(groupOrAttrs{group: name})
}

func (h *SQLogger) Close() {
	h.db.Close()
	return
}

func (h *SQLogger) appendAttr(buf []byte, a slog.Attr, keyColor *color.Color) []byte {
	// Resolve the Attr's value before doing anything else.
	a.Value = a.Value.Resolve()
	// Ignore empty Attrs.
	if a.Equal(slog.Attr{}) {
		return buf
	}
	switch a.Value.Kind() {
	case slog.KindString:

		buf = fmt.Appendf(buf, "%s%q ", keyColor.Sprint(a.Key+"="), a.Value.String())

	case slog.KindTime:
		// Write times in a standard way, without the monotonic time.
		if a.Key == slog.TimeKey {
			buf = fmt.Appendf(buf, "%s ", a.Value.Time().Format(time.RFC3339Nano))
			break
		}

		buf = fmt.Appendf(buf, "%s%s ", keyColor.Sprint(a.Key+"="), a.Value.Time().Format(time.RFC3339Nano))
	case slog.KindGroup:
		attrs := a.Value.Group()
		// Ignore empty groups.
		if len(attrs) == 0 {
			return buf
		}
		for _, ga := range attrs {

			if a.Key != "" {
				buf = fmt.Appendf(buf, "%s.", a.Key)
			}

			buf = h.appendAttr(buf, ga, keyColor)
		}
	default:
		if a.Key == slog.LevelKey {
			buf = fmt.Appendf(buf, "%s ", a.Value.String())
			break
		}
		buf = fmt.Appendf(buf, "%s%s ", keyColor.Sprint(a.Key+"="), a.Value)
	}
	return buf
}

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1024)
		return &b
	},
}

func allocBuf() *[]byte {
	return bufPool.Get().(*[]byte)
}

func freeBuf(b *[]byte) {
	// To reduce peak allocation, return only smaller buffers to the pool.
	const maxBufferSize = 16 << 10
	if cap(*b) <= maxBufferSize {
		*b = (*b)[:0]
		bufPool.Put(b)
	}
}
