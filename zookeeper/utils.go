package zookeeper

import (
	"errors"
	"github.com/go-zookeeper/zk"
	"os"
	"strings"
)

func CreateFullPath(conn *zk.Conn, path string, data []byte, flags int32) (string, error) {
	parts := splitPath(path)
	// drop last part
	parts = parts[:len(parts)-1]
	currentPath := ""
	for _, part := range parts {
		currentPath += "/" + part
		exists, _, err := conn.Exists(currentPath)
		if err != nil {
			return "", err
		}
		if !exists {
			_, err = conn.Create(currentPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
			if err != nil && !errors.Is(err, zk.ErrNodeExists) {
				return "", err
			}
		}
	}

	if data == nil {
		host, err := os.Hostname()
		if err != nil {
			return "", err
		}

		data = []byte(host)
	}
	return conn.Create(path, data, flags, zk.WorldACL(zk.PermAll))
}

func splitPath(path string) []string {
	parts := strings.Split(path, "/")

	var nonEmptyParts []string
	for _, part := range parts {
		if part != "" {
			nonEmptyParts = append(nonEmptyParts, part)
		}
	}

	return nonEmptyParts
}
