package main

import (
	"encoding/binary"
	"flag"
	"log"
	"net"
	"strconv"
	"time"
)

const (
	TYPE = "tcp"
)

const (
	subscribe   = 'a'
	unsubscribe = 'b'
	connect     = 'c'
	disconnect  = 'd'
	publish     = 'e'
	destroy     = 'f'
	//dispatch    = 'g'
)

type message struct {
	typ  int32
	data any
}

type subscribeMessage struct {
	clients  []string
	channels []string
}

type publishMessage struct {
	channel string
	msg     string
}

type unsubscribeMessage struct {
	clients  []string
	channels []string
}

type connectMessage struct {
	clients []string
	channel chan message
}

type disconnectMessage struct {
	clients []string
}

type destroyMessage struct {
	channel chan message
}

type worker struct {
	receiver      chan message
	clientMapping map[string]map[chan message]struct{} // mapping clients to connections (tcp connections)
	subMapping    map[string]map[string]struct{}       // mapping of channels to clients
}

func makeWorker() worker {
	return worker{
		receiver:      make(chan message),
		clientMapping: make(map[string]map[chan message]struct{}),
		subMapping:    make(map[string]map[string]struct{}),
	}
}

func (w *worker) publish(channel string, msg message) {
	clients := w.subMapping[channel]
	for k := range clients {
		channels := w.clientMapping[k]
		for c := range channels {
			c <- msg
		}
	}
}

func (w *worker) subscribe(clients []string, channels []string) {
	for _, client := range clients {
		for _, channel := range channels {
			if x := w.subMapping[channel]; x == nil {
				w.subMapping[channel] = make(map[string]struct{})
			}
			w.subMapping[channel][client] = struct{}{}
		}
	}
}

func (w *worker) unsubscribe(clients []string, channels []string) {
	for _, channel := range channels {
		for _, client := range clients {
			delete(w.subMapping[channel], client)
		}
	}
}

func (w *worker) connect(clients []string, connection chan message) {
	for _, client := range clients {
		if cl := w.clientMapping[client]; cl == nil {
			w.clientMapping[client] = make(map[chan message]struct{})
		}
		w.clientMapping[client][connection] = struct{}{}
	}
}

func (w *worker) disconnect(clients []string) {
	for _, client := range clients {
		// TODO
		// WARNING connection is unaware of disconnecting
		delete(w.clientMapping, client)
	}
}

func (w *worker) destroy(chanl chan message) {
	for _, b := range w.clientMapping {
		delete(b, chanl)
	}
}

func (w *worker) work() {
	for {
		msg := <-w.receiver

		switch msg.typ {
		case subscribe:
			data := msg.data.(subscribeMessage)
			w.subscribe(data.clients, data.channels)
		case unsubscribe:
			data := msg.data.(unsubscribeMessage)
			w.unsubscribe(data.clients, data.channels)
		case publish:
			data := msg.data.(publishMessage)
			w.publish(data.channel, msg)
		case connect:
			data := msg.data.(connectMessage)
			w.connect(data.clients, data.channel)
		case disconnect:
			data := msg.data.(disconnectMessage)
			w.disconnect(data.clients)
		case destroy:
			data := msg.data.(destroyMessage)
			w.destroy(data.channel)
		default:
			println("unknown message", msg.typ)
		}
	}
}

func main() {
	host := flag.String("host", "127.0.0.1", "host name")
	port := flag.Uint("port", 9001, "port number")
	flag.Parse()

	worker := makeWorker()
	go worker.work()

	listen, err := net.Listen(TYPE, *host+":"+strconv.Itoa(int(*port)))
	if err != nil {
		log.Fatal(err)
	}
	// close listener
	defer listen.Close()
	println("listening", *host+":"+strconv.Itoa(int(*port)))
	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handleIncomingRequest(conn, worker.receiver)
	}
}

func parseString(conn *net.Conn) (string, error) {
	err := (*conn).SetReadDeadline(time.Now().Add(time.Second * 5))
	if err != nil {
		println("failed to remove timeout", (*conn).RemoteAddr().String())
		return "", err
	}
	buf := make([]byte, 4)

	_, err = (*conn).Read(buf)
	if err != nil {
		println("failed to read", (*conn).RemoteAddr().String())
		return "", err
	}

	size := binary.BigEndian.Uint32(buf)
	str := make([]byte, size)
	_, err = (*conn).Read(str)
	if err != nil {
		println("read timeout", (*conn).RemoteAddr().String())
		return "", err
	} else {
		return string(str), nil
	}
}

func parseArray(conn *net.Conn) ([]string, error) {
	buf := make([]byte, 4)

	_, err := (*conn).Read(buf)
	if err != nil {
		println("failed to read", (*conn).RemoteAddr().String())
		return nil, err
	}

	size := binary.BigEndian.Uint32(buf)
	strings := make([]string, size)
	for i := range strings {
		str, err := parseString(conn)
		if err != nil {
			return nil, err
		}
		strings[i] = str
	}
	return strings, nil
}

func parseSubscribe(conn *net.Conn, woke *chan message) error {
	clients, err := parseArray(conn)
	if err != nil {
		return err
	}
	channels, err := parseArray(conn)
	if err != nil {
		return err
	}
	*woke <- message{
		typ: subscribe,
		data: subscribeMessage{
			clients:  clients,
			channels: channels,
		},
	}
	println("parsed subscribe")
	return nil
}

func parsePublish(conn *net.Conn, woke *chan message) error {
	channel, err := parseString(conn)
	if err != nil {
		return err
	}
	msg, err := parseString(conn)
	if err != nil {
		return err
	}

	*woke <- message{
		typ: publish,
		data: publishMessage{
			channel: channel,
			msg:     msg,
		},
	}
	return nil
}

func parseConnect(conn *net.Conn, woke *chan message, chanl chan message) error {
	clients, err := parseArray(conn)
	if err != nil {
		return err
	}

	*woke <- message{
		typ: connect,
		data: connectMessage{
			clients: clients,
			channel: chanl,
		},
	}
	return nil
}

func parseUnsubscribe(conn *net.Conn, woke *chan message) error {
	clients, err := parseArray(conn)
	if err != nil {
		return err
	}
	channels, err := parseArray(conn)
	if err != nil {
		return err
	}
	*woke <- message{
		typ: unsubscribe,
		data: unsubscribeMessage{
			clients:  clients,
			channels: channels,
		},
	}
	return nil
}

func parseDisconnect(conn *net.Conn, woke *chan message) error {
	clients, err := parseArray(conn)
	if err != nil {
		return err
	}

	*woke <- message{
		typ: disconnect,
		data: disconnectMessage{
			clients: clients,
		},
	}
	return nil
}

func encodeString(str string) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(len(str)))
	buf = append(buf, []byte(str)...)
	return buf
}

func dispatchEvent(conn *net.Conn, msg publishMessage) error {
	println("fake dispatch", msg.channel, msg.msg)

	_, err := (*conn).Write(encodeString(msg.channel))
	if err != nil {
		return err
	}
	_, err = (*conn).Write(encodeString(msg.msg))
	if err != nil {
		return err
	}
	return nil
	// (*conn).Write()
}

func handleClientSend(conn *net.Conn, client chan message) {
	defer close(client)
	for {
		mes := <-client
		switch mes.typ {
		case destroy:
			return
		case publish:
			data := mes.data.(publishMessage)
			if err := dispatchEvent(conn, data); err != nil {
				return
			}
		default:
			println("unknown message type", mes.typ)
		}
	}
}

func handleIncomingRequest(conn net.Conn, woke chan message) {
	println("creating connection", conn.RemoteAddr().String())
	cha := make(chan message)
	buffer := make([]byte, 1)
	go handleClientSend(&conn, cha)

	defer conn.Close()
	defer func() {
		println("destroying connection", conn.RemoteAddr().String())
		woke <- message{
			typ:  destroy,
			data: destroyMessage{channel: cha},
		}
		cha <- message{
			typ:  destroy,
			data: nil,
		}
	}()

	for {
		err := conn.SetReadDeadline(time.Time{})
		if err != nil {
			println("failed to remove timeout", conn.RemoteAddr().String())
			return
		}
		if n, err := conn.Read(buffer); err == nil {
			println("received message", buffer[0], conn.RemoteAddr().String(), n)
			switch buffer[0] {
			case connect:
				err := parseConnect(&conn, &woke, cha)
				if err != nil {
					return
				}
			case disconnect:
				err := parseDisconnect(&conn, &woke)
				if err != nil {
					return
				}
			case publish:
				err := parsePublish(&conn, &woke)
				if err != nil {
					return
				}
			case subscribe:
				err := parseSubscribe(&conn, &woke)
				if err != nil {
					return
				}
			case unsubscribe:
				err := parseUnsubscribe(&conn, &woke)
				if err != nil {
					return
				}
			default:
				println("received unknown message", buffer[0], conn.RemoteAddr().String())
			}
		} else {
			println("failed to read message", err.Error(), conn.RemoteAddr().String())
			return
		}
	}
}
