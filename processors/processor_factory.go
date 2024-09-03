package processors

import (
	"errors"
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"reflect"
)

var ErrProcessorTypeNotFound = errors.New("processor type not found")

type processorTypeNotFound struct {
	Type string
}

func (e *processorTypeNotFound) Error() string {
	return fmt.Sprintf("processor type %s not found", e.Type)
}

func (e *processorTypeNotFound) Is(target error) bool {
	return target == ErrProcessorTypeNotFound
}

func newProcessorTypeNotFoundError(t string) error {
	return &processorTypeNotFound{
		Type: t,
	}
}

// DefaultProcessorFactory is an implementation of ProcessorFactory.
type DefaultProcessorFactory struct {
	processorMap map[string]reflect.Type
}

func (f *DefaultProcessorFactory) RegisterProcessorWithTypeName(typeName string, processor definitions.Processor) {
	f.processorMap[typeName] = reflect.TypeOf(processor).Elem()
}

// NewDefaultProcessorFactory creates a new DefaultProcessorFactory.
func NewDefaultProcessorFactory() definitions.ProcessorFactory {
	return &DefaultProcessorFactory{
		processorMap: make(map[string]reflect.Type),
	}
}

// RegisterProcessor registers a processor type with the factory.
func (f *DefaultProcessorFactory) RegisterProcessor(processor definitions.Processor) {
	typeName := f.getTypeName(processor)
	f.processorMap[typeName] = reflect.TypeOf(processor).Elem()
}

func (f *DefaultProcessorFactory) GetProcessor(typeName string) (definitions.Processor, error) {
	processorType, exists := f.processorMap[typeName]
	if !exists {
		return nil, newProcessorTypeNotFoundError(typeName)
	}

	processorInstance := reflect.New(processorType).Interface().(definitions.Processor)
	return processorInstance, nil
}

// getTypeName returns the fully qualified type name of a processor.
func (f *DefaultProcessorFactory) getTypeName(processor definitions.Processor) string {
	processorType := reflect.TypeOf(processor).Elem()
	return fmt.Sprintf("%s.%s", processorType.PkgPath(), processorType.Name())
}
