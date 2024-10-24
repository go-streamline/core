package zookeeper

import (
	"errors"
	"github.com/go-zookeeper/zk"
	"os"
	"strings"
)

// CreateFullPath creates a path in Zookeeper and all its parent nodes if they do not exist.
// If data is nil, the hostname of the current machine is used as the data.
// Returns the created path and the data as a string.
func CreateFullPath(conn *zk.Conn, path string, data []byte, flags int32) (string, string, error) {
	parts := splitPath(path)
	// drop last part
	parts = parts[:len(parts)-1]
	currentPath := ""
	for _, part := range parts {
		currentPath += "/" + part
		exists, _, err := conn.Exists(currentPath)
		if err != nil {
			return "", "", err
		}
		if !exists {
			_, err = conn.Create(currentPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
			if err != nil && !errors.Is(err, zk.ErrNodeExists) {
				return "", "", err
			}
		}
	}

	if data == nil {
		host, err := os.Hostname()
		if err != nil {
			return "", "", err
		}

		data = []byte(host)
	}
	createdPath, err := conn.Create(path, data, flags, zk.WorldACL(zk.PermAll))
	return createdPath, string(data), err
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
