package file

import (
	"fmt"
	"time"

	"github.com/hpcloud/tail"
	"github.com/spartanlogs/spartan/config"
	"github.com/spartanlogs/spartan/event"
	"github.com/spartanlogs/spartan/inputs"
	"github.com/spartanlogs/spartan/utils"
	"gopkg.in/tomb.v2"
)

func init() {
	inputs.Register("file", newFileInput)
}

type fileConfig struct {
	path string
}

var fileConfigSchema = []config.Setting{
	{
		Name:     "path",
		Type:     config.String,
		Required: true,
	},
}

// A FileInput will read a file and optionally tail it. Each line is considered
// a separate event.
type FileInput struct {
	inputs.BaseInput
	config *fileConfig
	t      tomb.Tomb
	out    chan<- *event.Event
}

func newFileInput(options utils.InterfaceMap) (inputs.Input, error) {
	i := &FileInput{
		config: &fileConfig{},
	}
	return i, i.setConfig(options)
}

func (i *FileInput) setConfig(options utils.InterfaceMap) error {
	var err error
	options, err = config.VerifySettings(options, fileConfigSchema)
	if err != nil {
		return err
	}

	i.config.path = options.Get("path").(string)

	return nil
}

// Start the FileInput reading/tailing.
func (i *FileInput) Start(out chan<- *event.Event) error {
	i.out = out
	i.t.Go(i.run)
	return nil
}

// Close the FileInput
func (i *FileInput) Close() error {
	i.t.Kill(nil)
	return i.t.Wait()
}

func (i *FileInput) run() error {
	for {
		select {
		case <-i.t.Dying():
			return nil
		default:
		}

		t, err := tail.TailFile(i.config.path, tail.Config{
			Follow: true,
			ReOpen: true,
		})

		if err != nil {
			fmt.Println(err.Error())
			time.Sleep(500 * time.Millisecond)
			continue
		}

		var line *tail.Line

		for {
			select {
			case line = <-t.Lines:
				if line.Err != nil {
					fmt.Println(line.Err.Error())
					continue
				}
				i.out <- event.New(line.Text)
			case <-i.t.Dying():
				t.Stop()
				t.Cleanup()
				return nil
			}
		}
	}
}
