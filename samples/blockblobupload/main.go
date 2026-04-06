package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/faelp22/go-commons-libs/core/config"
	"github.com/faelp22/go-commons-libs/pkg/adapter/azure/blobstorage"
	"github.com/phuslu/log"
)

const (
	_      = iota
	KB int = 1 << (10 * iota)
	MB
	GB
	TB
	PB
)

func unitConverter(size int) string {
	unit := ""
	value := float64(size)
	switch {
	case size >= PB:
		unit = "PB"
		value = float64(size / PB)
	case size >= TB:
		unit = "TB"
		value = float64(size / TB)
	case size >= GB:
		unit = "GB"
		value = float64(size / GB)
	case size >= MB:
		unit = "MB"
		value = float64(size / MB)
	case size >= KB:
		unit = "KB"
		value = float64(size / KB)
	case size >= 0:
		unit = "B"
		value = float64(size)
	default:
		unit = "?"
	}

	return fmt.Sprintf("%.2f %s", value, unit)
}

func main() {
	conf := &config.Config{
		BlobStorage: &config.BlobStorage{},
	}
	blobStorageService := blobstorage.New(conf)

	flagFilePath := flag.String("filepath", "", "file path")
	flag.Parse()

	if *flagFilePath == "" {
		log.Fatal().Msg("filepath is required")
	}

	absFilePath, err := filepath.Abs(*flagFilePath)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	file, err := os.Open(absFilePath)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Error().Str("ERRO_BLOB", "error closing file").Msg(err.Error())
		}
	}(file)

	fileInfo, _ := file.Stat()

	log.Debug().Str("FILE_SIZE", unitConverter(int(fileInfo.Size()))).Msg("Tamanho do arquivo")
	log.Debug().Str("FILE_NAME", fileInfo.Name()).Msg("Nome do arquivo")

	fileSize := fileInfo.Size()

	blockBlobClient, err := blobStorageService.CreateBlockBlobClient(fileInfo.Name(), "test")
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	var chunkSize int64 = 4 << 20                    // 4MB
	chunks := (fileSize + chunkSize - 1) / chunkSize // ceil it
	if chunks > math.MaxUint16 {
		log.Fatal().Msgf("too many chunks: %d (max %d)", chunks, math.MaxUint16)
	}
	if chunks == 0 {
		chunks++
	}

	totalNumberOfChunks := uint16(chunks)

	ctx := context.Background()

	blockIDs := make([]string, 0, totalNumberOfChunks)

	for i := uint16(1); i <= totalNumberOfChunks; i++ {
		chunkData := make([]byte, chunkSize)
		n, err := file.Read(chunkData)
		if err != nil {
			log.Fatal().Msg(err.Error())
		}

		chunkData = chunkData[:n]

		log.Debug().Msg(fmt.Sprintf("upload chunk %d of %d, size %s\r", i, totalNumberOfChunks, unitConverter(len(chunkData))))

		blockID, err := blobStorageService.PutBlock(ctx, blockBlobClient, i, &chunkData)
		if err != nil {
			log.Fatal().Msg(err.Error())
		}

		log.Debug().Str("BlockID", blockID).Msg("success to upload chunk, store block")

		blockIDs = append(blockIDs, blockID)
	}

	err = blobStorageService.MountFile(ctx, blockBlobClient, &blockIDs)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	log.Info().Msg("Success to upload file")
}
