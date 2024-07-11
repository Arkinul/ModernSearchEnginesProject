# Usage

Crawler CLI:
```
python -m crawl.cli
```

Requirements (nach-)installieren:
```
pip install -r requirements.txt
```


# To Do:

- [x] Fester Termin (regulärer Vorlesungstermin)
  - DI: 16-18
  - DO: 14:15-18 Uhr

- [x] Repository erstellen —> Heinrich

- [x] Wie wollen wir das alles speichern? Leightweight SQL Datenbank —> SQLite

- [ ] Idee Crawling Gimmick / "Innovation"
  - auch PDFs crawlen & indizieren

- [ ] Idee User Interface/Gimmick
  - Mindmap/wordcloud?



Crawling
- [x] Requests fertig (Spracherkennung im HTTP Header etc.) (Heinrich)
- [ ] Requests verfeinern (Timeouts etc.)
- [x] Links im Parsing (Heinrich)
- [x] relevance check testen/tunen (Petros)
- [ ] Crawling zusammensetzen(Constantin)
- [ ] robots.txt respektieren (Albert)
  - Hostdatenbank
  - library (https://docs.python.org/3/library/urllib.robotparser.html ?)
  - cache / index robots.txt (maybe processsed) per host
  - extract host from URL (public suffix list)
- [ ] Docs in DB (main vom crawler) (Constantin)
- [ ] duplicate check (Vergleich/speichern der simhashes) (Constantin)
- [x] URL Normalisierung (Albert)
  - überlegen, wie man das testet
- [ ] last modified speichern (Heinrich)
- [ ] URL checken, ob schon besucht (Albert)
  - (Seiten recrawlen, regelmäßig)
- [x] crawler.py aufsplitten in mehrere Dateien
- [ ] Request-Log in DB
  - mit Status, Content (raw)
  - doc ist Abstraktion von Request ( oder so ähnlich)

Indexing
- [ ] Datenbankschema 
- [ ] tokenization
  - mit Library hoffentlich
- [ ] Wortliste
- [ ] Inverted index Tabelle
- [ ] evtl. bigrams + fielded

Retrieval
- [ ] erstmal BM25 (Albert)
- [ ] evtl. LDA (Albert)

Interface
- [x] erstmal funktionsfähig, dann fancy (Luisa)
- [ ] Icons 
- [ ] evtl. Boxen oder so
- [ ] verbinden mit Rest
  - klickbar
  - echte Ergebnisse

Report
- [ ] die ganze Zeit Notizen machen, möglichst
