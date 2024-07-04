To Do:

- [x] Fester Termin (regulärer Vorlesungstermin)
  - DI: 16-18
  - DO: 14:15-18 Uhr

- [x] Repository erstellen —> Heinrich

- [x] Wie wollen wir das alles speichern? Leightweight SQL Datenbank —> SQLite

- [ ] Idee User Interface/Gimmick
  - Mindmap/wordcloud?



Crawling
- [ ] Requests fertig (Spracherkennung etc.) (Heinrich)
- [ ] Links im Parsing (Heinrich)
- [ ] relevance check testen/tunen (Petros)
- [ ] robots.txt respektieren (Albert)
  - library (https://docs.python.org/3/library/urllib.robotparser.html ?)
  - cache / index robots.txt (maybe processsed) per host
  - extract host from URL (public suffix list)
- [ ] Docs in DB (main vom crawler) (Constantin)
- [ ] duplicate check (Vergleich/speichern der simhashes) (Constantin)
- [ ] URL Normalisierung (Albert)
  - überlegen, wie man das testet
- [ ] URL checken, ob schon besucht (Albert)
  - (Seiten recrawlen, regelmäßig)
- [ ] crawler.py aufsplitten in mehrere Dateien

Indexing
- [ ] tokenization
- [ ] Wortliste
- [ ] Inverted index tabelle

Retrieval
- [ ] erstmal BM25

Interface
- [ ] erstmal funktionsfähig, dann fancy (Luisa)

Report
- [ ] die ganze Zeit Notizen machen, möglichst
