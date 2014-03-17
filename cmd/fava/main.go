// TODO: add version info

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"time"

	"github.com/kr/beanstalk"
)

func UsageExit(status int) {
	f := os.Stderr
	if status == 0 {
		f = os.Stdout
	}
	fmt.Fprintln(f, "fava [ run | graph ] conf.json")
	os.Exit(status)
}

type Conf []Task

type Task struct {
	In, Out []string
	Cmd     []string
	FanOut  bool
}

type TubeMap map[string][]*Task

func (m TubeMap) Select(tube string) *Task {
	tasks := m[tube]
	idx := rand.Intn(len(tasks))
	return tasks[idx]
}

func LoadConf(file string) (c Conf, err error) {
	// TODO: check for command presence
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	err = json.NewDecoder(f).Decode(&c)
	if err != nil {
		return nil, err
	} else if len(c) == 0 {
		return nil, fmt.Errorf("Nothing to do (empty conf?)")
	}
	return c, nil
}

func BuildTubeMap(conf Conf) (TubeMap, error) {
	// TODO: check for duplicate tube refs
	// TODO: refactor this and LoadConf to have more sensible local checks
	m := make(TubeMap, len(conf))
	for i := range conf {
		task := &conf[i]
		if len(task.In) == 0 {
			return nil, fmt.Errorf("conf entries must have non-empty 'in' arrays")
		} else if len(task.Cmd) == 0 && len(task.Out) == 0 {
			return nil, fmt.Errorf("conf entries must have either/both 'cmd' or 'out' fields")
		}
		for _, tube := range task.In {
			m[tube] = append(m[tube], task)
		}
	}
	return m, nil
}

func setMap(m map[string]string, key, pre string) string {
	if v, ok := m[key]; ok {
		return v
	}
	v := fmt.Sprint(pre, len(m))
	m[key] = v
	fmt.Printf("\t%s [label=%q];\n", v, key)
	return v
}

func Graph() {
	if len(os.Args) != 3 {
		UsageExit(2)
	}
	arg := os.Args[2]
	if arg == "-h" {
		UsageExit(0)
	}
	log.SetFlags(0)
	conf, err := LoadConf(arg)
	if err != nil {
		log.Fatalln("conf load failed:", err)
	}
	// sanity check. may remove later
	_, err = BuildTubeMap(conf)
	if err != nil {
		log.Fatalln("could not construct tube map:", err)
	}
	fmt.Printf("digraph %q {\n", arg)
	//fmt.Println("\tnode [shape=ellipse];")
	m := make(map[string]string)
	for i, task := range conf {
		c := fmt.Sprint("c", i)
		for _, tube := range task.In {
			v := setMap(m, tube, "t")
			fmt.Printf("\t%s -> %s;\n", v, c)
		}
		label := ""
		if task.FanOut {
			label = " [style=dotted]"
		}
		for _, tube := range task.Out {
			v := setMap(m, tube, "t")
			fmt.Printf("\t%s -> %s%s;\n", c, v, label)
		}
		label = "<mux>"
		if len(task.Cmd) > 0 {
			label = strings.Join(task.Cmd, " ")
		}
		fmt.Printf("\t%s [shape=box label=%q];\n", c, label)
	}
	fmt.Println("}")
}

func Dispatch() {
	if len(os.Args) != 3 {
		UsageExit(2)
	}
	arg := os.Args[2]
	if arg == "-h" {
		UsageExit(0)
	}
	log.SetFlags(0)
	conf, err := LoadConf(arg)
	if err != nil {
		log.Fatalln("conf load failed:", err)
	}
	tubeMap, err := BuildTubeMap(conf)
	if err != nil {
		log.Fatalln("could not construct tube map:", err)
	}
	conn, err := beanstalk.Dial("tcp", "localhost:11300")
	if err != nil {
		log.Fatalln("beanstalkd connection failed:", err)
	}
	{
		tubes := make([]string, 0, len(tubeMap))
		for tube := range tubeMap {
			tubes = append(tubes, tube)
		}
		conn.TubeSet = *beanstalk.NewTubeSet(conn, tubes...)
	}

	log.SetFlags(log.LstdFlags)

	sigs := make(chan os.Signal)

	// SIGINT will cause a graceful shutdown
	signal.Notify(sigs, os.Interrupt)

	// TODO: add flag for numworkers. Fix w.r.t. reserve
	//env := newEnv(conn, runtime.NumCPU(), tubeMap)
	env := newEnv(conn, 1, tubeMap)

	// TODO: catch abnormal exit
	<-sigs
	log.Println("Received interrupt signal... Exiting gracefully")
	env.Quit()
	log.Println("Done")
}

type job struct {
	id   uint64
	tube string
	data []byte
}

func newEnv(conn *beanstalk.Conn, numworkers int, tubeMap TubeMap) *env {
	if numworkers < 1 {
		panic("numworkers must be at least 1")
	}
	env := &env{
		conn: conn,
		tmap: tubeMap,
		work: make(chan chan<- job, numworkers),
		quit: make(chan struct{}),
	}
	go env.watch()
	for i := 0; i < numworkers; i++ {
		go env.handle()
	}
	return env
}

type env struct {
	conn *beanstalk.Conn
	tmap TubeMap
	work chan chan<- job
	quit chan struct{}
}

func (e env) Quit() {
	close(e.quit)
	for i := 0; i < cap(e.work); i++ {
		<-e.work
	}
}

func (e env) watch() {
	for {
		var worker chan<- job
		select {
		case <-e.quit:
			return
		case worker = <-e.work:
		}
		id, data, err := e.conn.Reserve(10 * time.Second)
		switch berr := err.(type) {
		case nil:
		case beanstalk.ConnError:
			switch berr.Err {
			case beanstalk.ErrDeadline:
				log.Println("Job taking too long")
			case beanstalk.ErrTimeout:
				// Critical: put the worker back in line
				e.work <- worker
				continue
			}
			log.Println("Reservation error:", err)
			return
		default:
			log.Println("Reservation unknown error:", err)
			return
		}
		m, err := e.conn.StatsJob(id)
		if err != nil {
			log.Println("StatsJob error:", err)
			return
		}
		tube, ok := m["tube"]
		if !ok {
			log.Println("StatsJob didn't return tube")
			return
		}
		worker <- job{id, tube, data}
	}
}

func (e env) handle() {
	ch := make(chan job)
	var obuf, ebuf bytes.Buffer
loop:
	for {
		obuf.Reset()
		ebuf.Reset()
		e.work <- ch
		var job job
		select {
		case <-e.quit:
			break loop
		case job = <-ch:
		}
		tube := job.tube
		task := e.tmap.Select(tube)
		dats := [][]byte{job.data}
		if len(task.Cmd) > 0 {
			cmd := exec.Command(task.Cmd[0], task.Cmd[1:]...)
			cmd.Stdin = bytes.NewReader(job.data)
			cmd.Stdout = &obuf
			cmd.Stderr = &ebuf
			if err := cmd.Run(); err != nil {
				log.Printf("Command failed: %v\n%v\n%s", err, task.Cmd, ebuf.Bytes())
				continue
			}
			dats = append(dats[:0], obuf.Bytes())
		}
		if task.FanOut {
			dats = splitBounds(dats[0])
		}
		for _, tube := range task.Out {
			for _, data := range dats {
				_, err := send(e.conn, tube, data)
				if err != nil {
					log.Printf("Job put failed [%s]: %v\n%s", tube, err, data)
				}
			}
		}
		err := e.conn.Delete(job.id)
		if err != nil {
			log.Printf("Could not delete job %d: %v", job.id, err)
		}
	}
	e.work <- ch
}

func send(conn *beanstalk.Conn, tube string, data []byte) (id uint64, err error) {
	conn.Tube.Name = tube
	return conn.Put(data, 1, 0, 1*time.Minute)
}

func splitBounds(data []byte) [][]byte {
	j := 1 + bytes.IndexByte(data, '\n')
	if j <= 1 || data[0] != '-' || data[1] != '-' {
		// missing or invalid boundary -- treat as single message
		return [][]byte{data}
	}
	bound := make([]byte, j+1)
	bound[0] = '\n'
	copy(bound[1:], data)
	var msgs [][]byte
	fmt.Printf("Bound: %q\n", bound)
	for {
		j--
		data = data[j:]
		if len(data) == 0 {
			break
		}
		j = bytes.Index(data, bound)
		if j < 0 {
			break
		} else if j == 0 {
			msgs = append(msgs, nil)
		} else {
			msgs = append(msgs, data[1:j])
		}
		j += len(bound)
	}
	msgs = append(msgs, data[1:])
	return msgs
}

func main() {
	if len(os.Args) < 2 {
		UsageExit(2)
	}
	switch os.Args[1] {
	case "run":
		Dispatch()
	case "graph":
		Graph()
	}
}
