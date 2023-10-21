package rdsmq

type ProducerOptions struct {
	msgQueueLen int
}

type ProducerOption func(p *ProducerOptions)

func WithMsgQueueLen(queueLen int) ProducerOption {
	return func(p *ProducerOptions) {
		p.msgQueueLen = queueLen
	}
}

func repairMsgQueueLen(p *ProducerOptions) {
	if p.msgQueueLen <= 0 {
		p.msgQueueLen = 500
	}
}
