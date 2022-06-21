package instrumentation

var (
	jaegerURL string = "http://localhost:14268/api/traces"
)

func JaegerURL() string {
	return jaegerURL
}

func SetJaegerURL(url string) {
	jaegerURL = url
}
