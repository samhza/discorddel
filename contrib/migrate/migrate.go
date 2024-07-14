package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"os"
	"path"
	"strconv"

	"github.com/diamondburned/arikawa/v3/discord"
	sqlite3 "github.com/mattn/go-sqlite3"
)

const schema = `
CREATE TABLE IF NOT EXISTS Message (
	id INTEGER NOT NULL PRIMARY KEY,
	author INTEGER NOT NULL,
	channel INTEGER NOT NULL,
	guild INTEGER,
	content TEXT NOT NULL,
	json TEXT NOT NULL
);
`

func main() {
	archive := flag.String("a", "archive", "archive directory")
	db, err := sql.Open("sqlite3", path.Join(*archive, "messages.db"))
	if err != nil {
		log.Fatalln(err)
	}
	_, err = db.Exec(schema)
	in, err := os.Open(path.Join(*archive, "messages"))
	if err != nil {
		log.Fatalln(err)
	}
	defer in.Close()
	sc := bufio.NewScanner(in)
	insert, err := db.Prepare("INSERT INTO Message (id, author, channel, guild, content, json) VALUES(?, ?, ?, ?, ?, ?)")
	if err != nil {
		log.Fatalln(err)
	}
	defer insert.Close()
	doesExist, err := db.Prepare("SELECT EXISTS(SELECT 1 FROM Message WHERE id = ?)")
	if err != nil {
		log.Fatalln(err)
	}
	defer doesExist.Close()
	for sc.Scan() {
		b := sc.Bytes()
		snowflakes, jsonb, _ := bytes.Cut(b, []byte(" "))
		splat := bytes.Split(snowflakes, []byte(","))
		mid, _ := strconv.ParseInt(string(splat[2]), 10, 64)
		_ = mid
		var exists bool
		err := doesExist.QueryRow(mid).Scan(&exists)
		if exists {
			continue
		}
		if err != nil {
			log.Fatalln(err)
		}
		var msg discord.Message
		err = json.Unmarshal(jsonb, &msg)
		if err != nil {
			log.Fatalln(err)
		}
		content := msg.Content
		msg.Content = ""
		jsonb, err = json.Marshal(msg)
		if err != nil {
			log.Fatalln(err)
		}
		guildID := sql.NullInt64{
			Int64: int64(msg.GuildID),
			Valid: msg.GuildID.IsValid(),
		}
		if _, err := insert.Exec(msg.ID, msg.Author.ID, msg.ChannelID, guildID, content, jsonb); err != nil {
			if e, ok := err.(sqlite3.Error); !ok || e.Code != sqlite3.ErrConstraint {
				log.Fatalln(err)
			}
		}
	}
	if err := sc.Err(); err != nil {
		log.Fatalln(err)
	}
}
