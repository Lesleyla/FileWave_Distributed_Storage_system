package message

// Message interprocess communication object
type Message struct {
	Mode      string
	BlockName string
	Content   []byte
	IpAddr    string
}
