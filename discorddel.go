package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"time"

	"github.com/diamondburned/arikawa/v3/api"
	"github.com/diamondburned/arikawa/v3/discord"
	"github.com/diamondburned/arikawa/v3/gateway"
	"github.com/diamondburned/arikawa/v3/session"
	"github.com/diamondburned/arikawa/v3/utils/httputil"
)

const (
	UnknownMessage                 httputil.ErrorCode = 10008
	SystemMessageActionUnavailable httputil.ErrorCode = 50021
	InvalidActionOnArchivedThread  httputil.ErrorCode = 50083
)

func main() {
	token := flag.String("token", "", "Discord user token")
	chid := flag.Uint64("channel", 0, "Discord channel ID")
	gid := flag.Uint64("guild", 0, "Discord guild ID")
	archive := flag.String("archive", "./archive", "Directory to log deleted messages in")
	flag.Parse()
	if *chid == 0 && *gid == 0 {
		flag.Usage()
		log.Fatalln("at least one of -channel and -guild must be specified")
	}
	if *token == "" {
		flag.Usage()
		log.Fatalln("-token option must be specified")
	}
	var output *output
	if *archive != "" {
		var err error
		output, err = newOutput(*archive)
		if err != nil {
			log.Fatalln("Error while opening archive directory:", err)
		}
		defer output.Close()
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()
	c := session.New(*token)
	self, err := c.Me()
	if err != nil {
		log.Fatalln("Error fetching self:", err)
	}
	pause := make(chan struct{})
	c.AddHandler(func(m *gateway.MessageCreateEvent) {
		if m.Author.ID == self.ID {
			pause <- struct{}{}
		}
	})
	if err := c.Open(ctx); err != nil {
		log.Fatalln(err)
	}
	defer c.Close()
	searchdata := api.SearchData{
		SortBy:    "timestamp",
		SortOrder: "asc",
		AuthorID:  self.ID,
	}
	var guildID discord.GuildID
	if *chid != 0 {
		chid := discord.ChannelID(*chid)
		searchdata.ChannelID = chid
		ch, err := c.Channel(chid)
		if err != nil {
			log.Fatalln("Error while fetching channel: ", err)
		}
		guildID = ch.GuildID
	} else {
		guildID = discord.GuildID(*gid)
	}
	now := time.Now()
	var deleted uint = 0
Outer:
	for {
		results, err := search(c.Client, guildID, searchdata)
		if err != nil {
			log.Fatalln("Error occured while searching messages:", err)
		}
		log.Printf("%d messages remaining.\n", results.TotalResults)
		if deleted > 0 {
			log.Printf("Estimated remaining time: %s\n", time.Since(now)/time.Duration(deleted)*time.Duration(results.TotalResults))
		}
		if results.TotalResults == 0 {
			break Outer
		}
		for _, result := range results.Messages {
			for _, m := range result {
			Inner:
				select {
				case <-pause:
					timer := time.NewTimer(30 * time.Second)
					for {
						select {
						case <-timer.C:
							break Inner
						case <-pause:
							timer.Reset(30 * time.Second)
						case <-ctx.Done():
							break Outer
						}
					}
				case <-ctx.Done():
					break Outer
				default:
				}
				m.GuildID = discord.GuildID(*gid)
				if output != nil {
					err := output.logMessage(m)
					if err != nil {
						log.Printf("Error logging message %s: %s", m.URL(), err)
					}
				}
				if m.Author.ID != self.ID {
					goto Continue
				}
				err = deleteMsg(c.Client, m)
				if err != nil {
					log.Printf("Error deleting %s: %s\n", m.URL(), err)
				}
			Continue:
				deleted++
				searchdata.MinID = m.ID + 1
			}
		}
	}
}

func newOutput(dir string) (*output, error) {
	o := new(output)
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		return nil, err
	}
	o.File, err = os.OpenFile(path.Join(dir, "messages"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	o.enc = json.NewEncoder(o.File)
	o.attdir = path.Join(dir, "attachments")
	return o, nil
}

type output struct {
	*os.File
	enc    *json.Encoder
	attdir string
}

func (o *output) logMessage(m discord.Message) error {
	var guild string
	if m.GuildID.IsNull() {
		guild = "dm"
	} else {
		guild = m.GuildID.String()
	}
	attd := path.Join(o.attdir, guild, m.ChannelID.String())
	err := os.MkdirAll(attd, 0777)
	if err != nil {
		return err
	}
	for n, att := range m.Attachments {
		attf := path.Join(attd, fmt.Sprintf("%d,%d %s",
			m.ID,
			n,
			att.Filename,
		))
		f, err := os.Create(attf)
		if err != nil {
			return fmt.Errorf("creating attachment file: %w", err)
		}
		resp, err := http.Get(att.URL)
		if err != nil {
			f.Close()
			return fmt.Errorf("requesting attachment contents: %w", err)
		}
		_, err = io.Copy(f, resp.Body)
		f.Close()
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("downloading attachment: %w", err)
		}
	}
	_, err = fmt.Fprintf(o, "%d,%d,%d ", m.GuildID, m.ChannelID, m.ID)
	if err != nil {
		return err
	}
	return o.enc.Encode(m)
}

func deleteMsg(c *api.Client, m discord.Message) error {
	err := c.DeleteMessage(m.ChannelID, m.ID, "")
	if err == nil {
		return nil
	}
	var derr *httputil.HTTPError
	if ok := errors.As(err, &derr); ok {
		switch derr.Code {
		case UnknownMessage:
			return nil
		case SystemMessageActionUnavailable:
			return nil
		case InvalidActionOnArchivedThread:
			msg, err := c.SendMessage(m.ChannelID, "\u200B")
			if err != nil {
				return fmt.Errorf("sending message to unarchive thread %s: %w", chanURL(m.GuildID, m.ChannelID), err)
			}
			err = c.DeleteMessage(m.ChannelID, m.ID, "")
			if err != nil {
				return fmt.Errorf("deleting unarchive-trigger message %s: %w", msg.URL(), err)
			}
			return deleteMsg(c, m)
		}
	}
	return err
}

func chanURL(gid discord.GuildID, cid discord.ChannelID) string {
	var g string
	if gid.IsNull() {
		g = "@me"
	} else {
		g = gid.String()
	}
	return fmt.Sprintf("https://discord.com/channels/%s/%s", g, cid)
}

func search(c *api.Client, guildID discord.GuildID, data api.SearchData) (api.SearchResponse, error) {
	var resp api.SearchResponse
	var endpoint string
	if guildID != 0 {
		endpoint = api.EndpointGuilds + guildID.String()
	} else {
		endpoint = api.EndpointChannels + data.ChannelID.String()
	}
	return resp, c.RequestJSON(
		&resp, "GET",
		endpoint+"/messages/search",
		httputil.WithSchema(c, data),
	)
}
