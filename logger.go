package pelichan

type genericLogger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
}

type voidLogger struct{}

func (l *voidLogger) Debug(args ...interface{})                 {}
func (l *voidLogger) Debugf(format string, args ...interface{}) {}
func (l *voidLogger) Info(args ...interface{})                  {}
func (l *voidLogger) Infof(format string, args ...interface{})  {}
func (l *voidLogger) Warn(args ...interface{})                  {}
func (l *voidLogger) Warnf(format string, args ...interface{})  {}
func (l *voidLogger) Error(args ...interface{})                 {}
func (l *voidLogger) Errorf(format string, args ...interface{}) {}
