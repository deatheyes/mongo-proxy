package proxy

type Handler interface {
	Handle(*Context)
}

type HandlerFunc func(*Context)
