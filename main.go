package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"text/template"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/hashicorp/go-multierror"
	"github.com/mildred/datamgr/util"
)

const (
	DatamgrFile      = "datamgr.yaml"
	DefaultMaxMemory = 32 << 20 // 32 MB

	FormatCodeYAML = 1

	TypeCodeString = 1
	TypeCodeBool   = iota

	GenerateCodeTimestamp = 1
)

type Config struct {
	Receive map[string]*ConfigReceive `yaml:"receive"`
}

type ConfigReceive struct {
	Fields     map[string]ConfigField `yaml:"fields"`
	CreateFile *ConfigCreateFile      `yaml:"create_file"`
}

type Process struct {
	*ConfigReceive
	Fields map[string]ConfigField
}

type ConfigField struct {
	Internal     bool        `yaml:"internal"`
	Value        interface{} `yaml:"value"`
	Type         string      `yaml:"type"`
	typeCode     int
	Generate     string `yaml:"generate"`
	generateCode int
	Required     bool   `yaml:"required"`
	Format       string `yaml:"format"`
}

type ConfigCreateFile struct {
	Name         string `yaml:"name"`
	nameTemplate *template.Template
	Format       string `yaml:"format"`
	formatCode   int
}

func main() {
	var server http.Server
	var config Config
	flag.StringVar(&server.Addr, "listen", ":8080", "Listen address")
	flag.Parse()

	ctx, stopContext := context.WithCancel(context.Background())
	util.CancelSignals(ctx, stopContext, util.StopSignals...)

	data, err := ioutil.ReadFile(DatamgrFile)
	if err != nil {
		log.Fatalf("Error reading %s: %v", DatamgrFile, err)
	}
	err = config.Parse(data)
	if err != nil {
		log.Fatalf("Error parsing %s: %v", DatamgrFile, err)
	}

	server.Handler = &config

	go func() {
		err := server.ListenAndServe()
		if err != nil {
			log.Fatalf("Error starting server: %v", err)
		}
	}()

	defer server.Shutdown(context.Background())

	<-ctx.Done()
}

func (c *Config) Parse(data []byte) error {
	err := yaml.Unmarshal(data, c)
	if err != nil {
		return err
	}

	for endpoint, r := range c.Receive {
		for fName, f := range r.Fields {
			switch f.Generate {
			case "":
			case "timestamp":
				f.generateCode = GenerateCodeTimestamp
				if f.Format == "" {
					f.Format = "20060102.150405.999999999"
				}
			default:
				err = multierror.Append(err, fmt.Errorf("receive[%+s].fields.%s.generate unexpected %v, expected \"timestamp\"", endpoint, fName, f.Generate)).ErrorOrNil()
			}
			switch f.Type {
			case "", "string":
				f.typeCode = TypeCodeString
			case "bool":
				f.typeCode = TypeCodeBool
			default:
				err = multierror.Append(err, fmt.Errorf("receive[%+s].fields.%s.type unexpected type %v, expected \"string\" or \"bool\"", endpoint, fName, f.Type)).ErrorOrNil()
			}
			r.Fields[fName] = f
		}
		if r.CreateFile != nil {
			r.CreateFile.nameTemplate = template.New("create_file.name")
			r.CreateFile.nameTemplate.Funcs(template.FuncMap{
				"field": func() string { return "" },
			})
			_, e := r.CreateFile.nameTemplate.Parse(r.CreateFile.Name)
			if e != nil {
				err = multierror.Append(err, fmt.Errorf("receive[%+s].create_file.name template error, %v", endpoint, e)).ErrorOrNil()
			}
			switch r.CreateFile.Format {
			case "yaml", "":
				r.CreateFile.formatCode = FormatCodeYAML
			default:
				err = multierror.Append(err, fmt.Errorf("receive[%+s].create_file.format unexpected format %v, expected \"yaml\"", endpoint, r.CreateFile.Format)).ErrorOrNil()
			}
		}
	}
	return err
}

func (c *Config) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler := c.Receive[r.URL.Path]
	if handler == nil {
		log.Printf("%s %s: 404 Not Found", r.Method, r.URL.Path)
		http.NotFound(w, r)
		return
	}
	log.Printf("%s %s", r.Method, r.URL.Path)
	handler.ServeHTTP(w, r)
}

func (c *ConfigReceive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(DefaultMaxMemory)
	if err == http.ErrNotMultipart {
		err = r.ParseForm()
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("Error parsing form: %v", err), http.StatusBadRequest)
		return
	}

	process := &Process{
		ConfigReceive: c,
		Fields:        map[string]ConfigField{},
	}

	for fieldName, field := range c.Fields {
		e := field.fetchValue(fieldName, r.Form)
		if e != nil {
			err = multierror.Append(err, e).ErrorOrNil()
		}
		process.Fields[fieldName] = field
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if c.CreateFile != nil {
		c.CreateFile.Perform(w, process)
	}

	if cb := r.Form.Get("callback"); cb != "" {
		http.Redirect(w, r, cb, http.StatusSeeOther)
		return
	}

	http.Redirect(w, r, r.Referer(), http.StatusSeeOther)
}

func (c *Process) fieldMap() map[string]interface{} {
	res := make(map[string]interface{})
	for name, field := range c.Fields {
		res[name] = field.Value
	}
	return res
}

func generateTimestamp(format string) string {
	if format == "" {
		format = time.RFC3339
	}
	return time.Now().UTC().Format(format)
}

func (f *ConfigField) fetchValue(name string, values url.Values) (err error) {
	v := values["field."+name]
	switch f.generateCode {
	case GenerateCodeTimestamp:
		f.Value = generateTimestamp(f.Format)
	}
	if f.Internal {
		return
	}
	if len(v) == 0 {
		if f.Required {
			err = fmt.Errorf("required field field.%s not set", name)
		}
		log.Printf("[DEBUG] Empty field.%s", name)
		return
	}
	switch f.typeCode {
	case TypeCodeString:
		f.Value = v[len(v)-1]
		break
	case TypeCodeBool:
		f.Value, err = strconv.ParseBool(v[len(v)-1])
		if err != nil {
			err = fmt.Errorf("cannot parse field field.%s to boolean (value is %+v)", name, v)
		}
		break
	}
	log.Printf("[DEBUG] Parse field.%s=%#v", name, f.Value)
	return
}

func (c *ConfigCreateFile) Perform(w http.ResponseWriter, r *Process) {
	var b bytes.Buffer
	t, err := r.CreateFile.nameTemplate.Clone()
	if err != nil {
		log.Printf("[ERROR] Failed to initialize file name template, %v", c.Name, err)
		http.Error(w, "Internal server error.", http.StatusInternalServerError)
		return
	}
	t.Funcs(template.FuncMap{
		"field": r.fieldMap,
	})
	err = t.Execute(&b, r)
	if err != nil {
		log.Printf("[ERROR] Failed to build file name from template %+v, %v", c.Name, err)
		http.Error(w, "Could not process request due to misconfiguration.", http.StatusInternalServerError)
		return
	}
	fileName := b.String()
	log.Printf("[DEBUG] Create file %v", fileName)

	dir := path.Base(fileName)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		log.Printf("[ERROR] Failed to create directory %v, %v", dir, err)
		http.Error(w, "Could not process request due to a system error, please try again later.", http.StatusInternalServerError)
		return
	}

	f, err := os.Create(fileName)
	if err != nil {
		log.Printf("[ERROR] Failed to create file %v, %v", fileName, err)
		http.Error(w, "Could not process request due to a system error, please try again later.", http.StatusInternalServerError)
		return
	}
	defer f.Close()

	switch c.formatCode {
	case FormatCodeYAML:
		err = yaml.NewEncoder(f).Encode(r.fieldMap())
	default:
		panic("Unexpected format")
	}
	if err != nil {
		log.Printf("[ERROR] Failed to encode file %v, %v", fileName, err)
		http.Error(w, "Could not process request because of data error.", http.StatusInternalServerError)
		return
	}
}
