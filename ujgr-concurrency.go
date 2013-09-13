package main

import (
	//"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	//"bytes"
	//"errors"
	"regexp"
	"runtime"
	"time"
	"sync"
	//"strings"
)

const SIZE int = 20971520

var chs []chan int
var cnum int
var chtmp chan int

func main() {
	if len(os.Args) > 1 {
		runtime.GOMAXPROCS(runtime.NumCPU())
		t := time.Now()
		chs = make([]chan int, 1)
		cnum = 0
		err := Tree(os.Args[1], 1)
		handleError(err)

		for _, ch := range chs {
			<-ch
		}
		fmt.Println(time.Since(t))
	} else {
		fmt.Println("Please input the Dir or file name")
	}
}

//list files under the dir
func Tree(dirname string, curHier int) error {
	dirAbs, err := filepath.Abs(dirname)
	handleError(err)
	fileInfos, err := ioutil.ReadDir(dirAbs)
	handleError(err)

	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			Tree(filepath.Join(dirAbs, fileInfo.Name()), curHier+1)
		} else {
			b := []byte(fileInfo.Name())
			//fmt.Println(fileInfo.Name())
			matched, _ := regexp.Match("[.](json.gz)$", b)
			//fmt.Println(matched)
			if matched {

				if cnum >= len(chs) {
					chs = append(chs, make(chan int))
				}
	
				chs[cnum] = make(chan int)

				go UTar(filepath.Join(dirAbs, fileInfo.Name()),chs[cnum])
				cnum += 1
				handleError(err)

			}

		}

	}
	return nil
}

func UTar(fpath string,ch chan int) error {
	
	fr, err := os.Open(fpath)
	handleError(err)
	defer fr.Close()

	gr, err := gzip.NewReader(fr)
	handleError(err)

	buf := make([]byte, SIZE)
	var num int = 0

	for {
		n, err := gr.Read(buf[num:])
		if err == io.EOF {
			break
		}
		num += n
		handleError(err)
	}

	fw, err := os.Create(gr.Header.Name)
	handleError(err)

	buf = buf[:num]

	_, err = fw.Write(buf)
	handleError(err)
	lock := &sync.Mutex{}
	lock.Lock()
	fmt.Println(fpath)
	fmt.Println("Success!")
	ch <- 1
	lock.Unlock()
	runtime.Gosched()
	return nil
}

func handleError(err error) {
	if err != nil && err != io.EOF {
		panic(err)
	}

}
