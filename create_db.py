import sqlite3
from nb_tokenizer import tokenize
from pathlib import Path
from argparse import ArgumentParser
from dataclasses import dataclass
import jsonlines
import uuid
import os

@dataclass
class Args:
    input_file: Path
    output_dir: Path
    
@dataclass
class _TokenParseResult:
    token: str
    sequence_number: int
    paragraph_number: int

def _args() -> Args:
    parser = ArgumentParser()
    parser.add_argument(
        "--input-file",
        type=Path,
        required=True,
        help="Path to the input file",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Path to the output dir",
    )
    args = parser.parse_args()
    return Args(
        input_file=args.input_file,
        output_dir=args.output_dir
    )

def _create_local_db(dbname):
    if os.path.exists(dbname):
        print("ERROR: Database already exists. Delete and re-run.")
        sys.exit(1)
    with sqlite3.connect(dbname) as dbcon:
        cur = dbcon.cursor()
        cur.execute("CREATE TABLE urns (urn INTEGER PRIMARY KEY, urntext text);")
        cur.execute("CREATE TABLE ft (urn int, word varchar, seq int, para int, page int, ordinal int);")
        cur.execute("CREATE TABLE metadata (dhlabid int, hash text, title text, domain text, responsible_editor bool, place text, county text, record_id text, warcpath text, timestamp text, uri text, langs text);")
        dbcon.commit()

def _write_to_local_database(dbname, token_tuples, metadata_tuple):
    with sqlite3.connect(dbname) as dbcon:
        cur = dbcon.cursor()
        cur.execute("INSERT INTO metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", (metadata_tuple))
        dbcon.commit()
        cur.execute("INSERT INTO urns VALUES (?, ?);", (metadata_tuple[0], f"{metadata_tuple[8]}#{metadata_tuple[7]}"))
        dbcon.commit()
        cur.executemany("INSERT INTO ft(urn, word, seq, para) VALUES(?, ?, ?, ?);", (token_tuples))
        dbcon.commit()

def _rename_db(dbname, output_dir):
    # first we get the min and max_id
    with sqlite3.connect(dbname) as dbcon:
        cur = dbcon.cursor()
        cur.execute("SELECT min(urn) as urn_min, max(urn) as urn_max FROM urns;")
        min_max = cur.fetchone()

    new_dbname = f"{output_dir}/alto_{min_max[0]}_{min_max[1]}.db"
    os.rename(dbname, new_dbname)


def _parse_tokens(fulltext: str) -> list[_TokenParseResult]:
    result = []
    paragraphs = fulltext.split("\n")
    sequence_number = 0
    for paragraph_number, paragraph in enumerate(paragraphs):
        tokens = tokenize(paragraph)
        for token in tokens:
            result.append(
                _TokenParseResult(
                    token=token,
                    sequence_number=sequence_number,
                    paragraph_number=paragraph_number,
                )
            )
            sequence_number += 1

    return result

def _read_jsonl(input_file):
    with jsonlines.open(input_file) as reader:
        for obj in reader:
            yield obj


def _main() -> None:
    args = _args()

    dbname = f"{args.output_dir}/{str(uuid.uuid4())}.db"
    _create_local_db(dbname)

    for x in _read_jsonl(args.input_file):
        metadata_fields = ["dhlabid", "hash", "title", "domain", "have-responsible-editor", "place", "county", "record_id", "warcpath", "timestamp", "uri", "langs"]
        metadata_tuple = tuple(x[field] for field in metadata_fields)
        tokens = _parse_tokens(x["full_text"])
        token_tuples = []

        for token in tokens:
            token_tuples.append((x["dhlabid"], token.token, token.sequence_number, token.paragraph_number))

        _write_to_local_database(dbname, token_tuples, metadata_tuple)        
    
    _rename_db(dbname, args.output_dir)

if __name__ == "__main__":
    _main()