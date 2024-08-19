import os
import sqlite3

with sqlite3.connect('db/metadata.db') as con:
	cur = con.cursor()
	cur.execute("CREATE TABLE metadata_core (dhlabid INT, title text, authors text, urn text, oaiid text, sesamid text, isbn10 text, city text, timestamp int, year int, publisher text, langs text, subjects text, ddc text, genres text, literaryform text, doctype text, ocr_creator text, ocr_timestamp int);")

for file in os.listdir('db'):
	if file.startswith("alto_"):
		with sqlite3.connect('db/' + file) as con2:
			cur2 = con2.cursor()
			cur2.execute("ATTACH DATABASE 'db/metadata.db' as meta;")
			cur2.execute("INSERT INTO meta.metadata_core (dhlabid, urn, authors, title, city, publisher, timestamp, year) select dhlabid, record_id, domain, uri, place, title, timestamp, substr(timestamp, 1, 4) from metadata;")
