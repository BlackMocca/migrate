package elasticsearch

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

const (
	INDEX_TEMPLATE = "${index}"
	BODY_TYPE_BULK = "bulk"
)

type Elasticsearch struct {
	Index      string
	RestConfig *RestConfig
}

type RestConfig struct {
	MigrationPath string            `json:"-"`
	Method        string            `json:"method"`
	Path          string            `json:"path"`
	Header        map[string]string `json:"header"`
	Body          interface{}       `json:"body"`
	BodyType      string            `json:"body_type"`
	BodyPathFile  string            `json:"body_path_file"`
}

func (r RestConfig) IsZero() bool {
	if r.Method == "" || r.Path == "" {
		return true
	}

	return false
}

func (r RestConfig) ToHTTPHeader() http.Header {
	h := http.Header{}
	h.Add("Content-Type", "application/json; charset=UTF-8")
	if r.Header == nil || len(r.Header) == 0 {
		return h
	}

	for key, val := range r.Header {
		h.Set(key, val)
	}

	return h
}

func (r *RestConfig) ReplaceStringWithIndex(index string) error {
	r.Path = strings.ReplaceAll(r.Path, INDEX_TEMPLATE, index)
	if r.Header != nil {
		for key, val := range r.Header {
			r.Header[key] = strings.ReplaceAll(val, INDEX_TEMPLATE, index)
		}
	}

	switch r.BodyType {
	case BODY_TYPE_BULK:
		pathFile := fmt.Sprintf("%s/%s", strings.Trim(r.MigrationPath, "/"), strings.Trim(r.BodyPathFile, "/"))
		bu, err := ioutil.ReadFile(pathFile)

		if err != nil {
			return err
		}

		r.Body = string(bu)
	default:
		bu, err := json.Marshal(r.Body)
		if err != nil {
			return err
		}

		str := strings.ReplaceAll(string(bu), INDEX_TEMPLATE, index)

		r.Body = str
	}

	return nil
}
