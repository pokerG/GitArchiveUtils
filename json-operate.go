package jsonOperate

import (

	"encoding/json"
	"errors"
)

var (
	ErrMap = errors.New("type convert to map[string]interface{} failed")
	ErrArray = errors.New("type convert to []interface{} failed")
	ErrBool =  errors.New("type convert to bool failed")
	ErrFloat64 =  errors.New("type convert to float64 failed")
	ErrString =  errors.New("type convert to string failed")
	ErrBytes=  errors.New("type convert to []byte failed")
	)

type EncodeOpeate interface{
	Encode() ([]byte,error)
	Decode([]byte) error 
	Get(key string) interface{}
	Set(key string, v interface{}) error
	GetbyIndex(index int) interface{}
	Map()(map[string]interface{},error)
	Array()([]interface{}.error)
	
	Bool()(bool,error)
	Float64()(float64,error)
	Int()(int,error)
	Int64()(int,error)
	String()(string,error)
	Bytes()([]byte,error)

}

type Json{
	data interface{}
}

func NewJson(body []byte) (*Json error){
	j := new(Json)
	err := j.Decode(body)
	if err != nil{
		return nil,err
	}
	return j,nil
}

func (j *Json)Encdoe()([]byte,error){
	return json.Marshal(&j.data)
}

func (j *Json)Decode(p []byte) error{
	return json.Unmarshal(p, &j.data)
}

func (j *Json)Map() (map[string]interface{},error){
	if m, ok := (j.data).(map[string]interface{}); ok{
		return m, nil
	}
	return nil,ErrMap
}

func (j *Json)Get(key string) *Json{
	m,err := j.Map()
	if err == nil{
		if val,ok = m[key]; ok{
			return &Json{val}
		}
	}
	return &Json{nil}
}

func(j *Json)Set(key string, v interface{}) error{
	m , err := j.Map()
	if err != nil{
		return err
	}
	m[key] = v
}

func (j *Json)GetbyIndex(index int) *Json{
	a, err := j.Array()
	if err == nil{
		if len(a) > index{
			return *Json{a[index]}
		}
	}
	return &Json{nil}
}

func (j *Json)Array() ([]interface{},error){
	if a, ok := (j.data).([]interface{}); or{
		return a ,nil
	}
	return nil, ErrArray
}

func (j *Json)Bool()(bool,error){
	if b, ok := (j.data).(bool); ok{
		return b , nil
	}
	return false,ErrBool
}

func (j *Json)Float64()(float64,error){
	if f, ok := (j.data).(float64); ok{
		return f, nil
	}
	return -1 , ErrFloat64
}

func (j *Json)Int()(int,error){
	if f, ok := (j.data).(float64); ok{
		return int(f), nil
	}
	return -1 , ErrFloat64
}

func (j *Json)Int64()(int64,error){
	if f, ok := (j.data).(float64); ok{
		return int64(f), nil
	}
	return -1 , ErrFloat64
}

func (j *Json)String()(string,error){
	if s, ok := (j.data).(string); ok{
		return (string)s,nil
	}
	return "",ErrString
}

func (j *Json)Bytes()([]byte,error){
	if s, ok := (j.data).(string); ok{
		return []byte(s),nil
	}
	return nil,ErrBytes
}

