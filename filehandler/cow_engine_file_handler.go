package filehandler

import (
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-streamline/interfaces/utils"
	"github.com/google/uuid"
	"io"
	"os"
	"path"
)

var NewEngineFileHandler = NewCopyOnWriteEngineFileHandler
var NewWriteOnlyEngineFileHandler = NewWriteOnlyCOWFileHandler
var ErrInputFileNotInitialized = fmt.Errorf("input file not initialized")
var ErrOutputFileNotInitialized = fmt.Errorf("output file not initialized")

type CopyOnWriteEngineFileHandler struct {
	input  string
	output string
	reader *os.File
	writer *os.File
}

func (c *CopyOnWriteEngineFileHandler) Read() (io.Reader, error) {
	if c.input == "" {
		return nil, ErrInputFileNotInitialized
	}
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
	if c.output == "" {
		return nil, ErrOutputFileNotInitialized
	}
	if c.writer != nil {
		return c.writer, nil
	}
	err := utils.CreateDirsIfNotExist(path.Dir(c.output))
	if err != nil {
		return nil, err
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
		if c.reader != nil {
			c.reader.Close()
			defer os.Remove(c.input)
		}
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

func NewWriteOnlyCOWFileHandler(output string) definitions.EngineFileHandler {
	return &CopyOnWriteEngineFileHandler{
		output: output,
	}
}

func generateNewOutputFilePath(input string) string {
	return path.Join(path.Dir(input), uuid.NewString())
}
