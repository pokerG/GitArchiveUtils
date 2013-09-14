package jsonOperate

import(
	"testing"
	"log"
	//"encoding/json"
	"os"
	"io/ioutil"
	//"errors"
)

func TestjsonOperate(t *testing.T) { 
	file, err := os.Open("test.json")
	if err != nil {
		t.Error("Can't open the test file")
	}
	defer file.Close()

	lines,_ := ioutil.ReadFile(file.Name())
	js ,err := NewJson(lines)

	ty,_ := js.Get("type").String()
	if ty != "PushEvent" {
		t.Error("First Get have problem")
	}

	id,_ := js.Get("repo").Get("id").Int()
	if id != 3055800 {
		t.Error("Second Get have problem")
	}

	url,_ := js.Get("repo").Get("url").String()
	if url != "https://api.github.dev/repos/knowledge-point/tinypm-backup"{
		t.Error("Convert have problem")
	}

	fl, err := os.OpenFile("log.txt",os.O_WRONLY,0666)
	if err != nil{
		t.Error("Can't open the log file")
	}
	defer fl.Close()
	log.SetOutput(fl)

	m, err := js.Map()
	if err != nil{
		t.Error(err.Error)
	}else{
		log.Panicln(m)
	}


	err = js.Get("repo").Set("name","test")
	if err != nil{
		t.Error(err.Error)
	}

	b, err := js.Encode()
	if err != nil{
		t.Error(err.Error)
	}else{
		log.Println(b)
	}

	log.Panicln("HHHHH")

}