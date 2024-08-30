package filehandler

import (
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"io"
	"os"
	"path"
)

var NewEngineFileHandler = NewCopyOnWriteEngineFileHandler

type CopyOnWriteEngineFileHandler struct {
	input  string
	output string
	reader *os.File
	writer *os.File
}

func (c *CopyOnWriteEngineFileHandler) Read() (io.Reader, error) {
	if c.reader != nil {
		return c.reader, nil
	}
	file, err := os.Open(c.input)
	if err != nil {
		return nil, err
	}
	c.reader = file
	return c.reader, nil
}

func (c *CopyOnWriteEngineFileHandler) Write() (io.Writer, error) {
	if c.writer != nil {
		return c.writer, nil
	}
	file, err := os.Create(c.output)
	if err != nil {
		return nil, err
	}
	c.writer = file
	return c.writer, nil
}

func (c *CopyOnWriteEngineFileHandler) Close() {
	if c.reader != nil {
		c.reader.Close()
		c.reader = nil
	}
	if c.writer != nil {
		c.writer.Close()
		c.writer = nil
	}
}

func (c *CopyOnWriteEngineFileHandler) GetInputFile() string {
	return c.input
}

func (c *CopyOnWriteEngineFileHandler) GetOutputFile() string {
	return c.output
}

func (c *CopyOnWriteEngineFileHandler) GenerateNewFileHandler() (definitions.EngineFileHandler, error) {
	input := c.input
	if c.writer != nil {
		input = c.output
		defer os.Remove(c.input)
	}

	c.Close()

	return &CopyOnWriteEngineFileHandler{
		input:  input,
		output: generateNewOutputFilePath(input),
	}, nil
}

func NewCopyOnWriteEngineFileHandler(input string) definitions.EngineFileHandler {
	return &CopyOnWriteEngineFileHandler{
		input:  input,
		output: generateNewOutputFilePath(input),
	}
}

func generateNewOutputFilePath(input string) string {
	return path.Join(path.Dir(input), uuid.NewString())
}
