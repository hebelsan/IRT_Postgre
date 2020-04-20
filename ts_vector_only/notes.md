## Notes

Originally i wanted to use the python library `wikipedia`. 
But this library is not able to search for pages of a certain category.
Therefore i decided to use the library `pymediawiki` instead.
`pymediawiki` is licensed under the MIT License.
Documentation: https://pymediawiki.readthedocs.io/en/latest/code.html

Problem: the results of the pymediawiki query do not 100% match with the results of the wikiepedia webpage query, most likely because of much less search parameter control



Bei der Benutzung der Wikipedia bibliothek stellte sich heraus, dass manche seiten keine sektion mit Kontent haben, sondern nur ein summary. Daher erstellte ich für jede seite eine zusätzliche sektion summary. Des Weiteren stellte sich heraus, dass manche sektionen nur als Überschrift für eine weitere Unterkategorie erstellt wurde und somit auch keinen Text enthält. Hier änderte ich den Code, sodass diese Sektionen mit keinem Inhalt nicht mit in die Datenbank aufgenommen werden.



## Rating Algorithmus

- Wie oft kommt das gesuchte Wort im Text vor (vergleich wie groß ist der Text generell)
- Was ist der minimale Abstand in dem das Wort vorkommt
- Wie oft kommt es maximal innerhalb einer Sektion vor
- Wörter im Titel werden höher gerated



## Mögliche Probleme

- Da ich nun die indexe des original textes beibehalte können diese sehr groß werden:
  was ist der maximale Index eines tsvectors (getestet mit der Ketegorie 'sports' und 2000 seiten):
  --> Position values in `tsvector` must be greater than 0 and no more than 16383 :/
  The reason for the limits are in the [source code](http://doxygen.postgresql.org/ts__type_8h_source.html) as MAXSTRLEN (and MAXSTRPOS). Text search vectors are stored in one long, continuous array up to 1 MiB in length (total of all characters for all unique lexemes). To access these, the ts_vector index structure allows 11 bits for word length and 20 bits for its position in the array. These limits allow the index structure fit into a 32-bit unsigned int.

  --> größters Dokument von 2000 Dokumenten hat 124169 Wörter in total, aber nur 8061 verschiedene lexeme (title: '2019 in sports')

  --> Im Durchschnitt sind die Dokumente 8780 Wörter lang
  --> Selbst wenn man nur die Lexeme zählt sind es maximal 19603 und im Durchschnitt  1478

  **Fazit:**
  Die Position eines ts_vectors kann ohne Weiteres praktisch nicht verwendet werden.
  Der ts_vector kann höchstens ohne position value verwendet werden.
  Wenn man jedoch die Position herausnimmt, werden auch die weights gelöscht.
  Ohne Position und weights ist der ts_vector für die ts_rank Funktion nicht mehr wirklich zu gebrauchen.
  Alles in allem scheint der ts_vector den Ansprüchen einer Information Retrievals Applikation nicht zu genügen.
  Er kann aller höchsten ohne position und ohne Ranking verwendet werden um die Dokument id herauszufinden in denen das gesuchte Wort(Wörter) vorkommen.
  Über eine zusätzliche Tabelle kann dann die Häufigkeit und Position der Wörter identifiziert werden.
  Jedoch muss auch eine ganz eigene Ranking Funktion geschrieben werden.

  **4 Mögliche weitere Vorgehen:**

  1. Für jedes Dokument eine Tabelle mit allen Lexemen und Positionen (Könnte kombiniert werden mit fuzzy search von tsvector um Dokument ids herauszubekommen)
  2. Eine Tabelle für alle Lexeme in welchen Dokumenten sie vorkommen und welchen Positionen
  3. Die Positionen für jeden Abschnitt neu bei Index 1 neu starten lassen, dann für jeden Abschnitt ein Ranking berechnen und dann die nzes Dokument kombinieren.
     Nachteil: Position der Wörter im ganzen Dokument wird vernachlässigt
  4. Einen Offset für jeden Abschnitt Speichern und dann mit einer Custom Funktion die Positionen des gesuchten Wortes eines Dokumentes herausfiltern

- Es könnte sein das das gesuchte Wort in jedem Abschnitt vorkommt und damit der erstellte ts-vector zu groß werden würde

- Die Stelle eines Wortes ist immer nur innerhalb eines Abschnittes eines Textes, es wäre eventuell gut noch einen offset mit zu speichern

- Wird die View automatisch geupdated?

- Aus der Doku: *Ranking can be expensive since it requires consulting the `tsvector` of each matching document, which can be I/O bound and therefore slow. Unfortunately, it is almost impossible to avoid since practical queries often result in large numbers of matches.*

- offset in den ts_vector einbauen + offset bei concatenation removen

- macht es einen unterschied im ranking an welcher stelle das gesuchte wort im dokument steht?
  also ob es ganz am anfang oder eher am ende oder in der mitte des Dokuments auftaucht?
  testen der rank funktion:

  

  select ts_rank_cd(to_tsvector('Free text seaRCh is a wonderful Thing'), to_tsquery('wonderful & thing'));

  --> rank = 0.1

  select ts_rank_cd(to_tsvector('a wonderful Thing Free text seaRCh is'), to_tsquery('wonderful & thing'));

  --> rank = 0.1

  

  select ts_rank(to_tsvector('Free text seaRCh is a wonderful Thing'), to_tsquery('wonderful & thing'));

  --> rank = 0.0991032

  select ts_rank(to_tsvector('wonderful Free text seaRCh is a Thing'), to_tsquery('wonderful & thing'));

  --> rank = 0.0852973

  

  select ts_rank(to_tsvector('This is just a test on how this will have an impact on the rank of the vector if the document is longer Free text seaRCh is a wonderful Thing'), to_tsquery('wonderful & thing'));

  --> rank = 0.0991032

  select ts_rank(to_tsvector('wonderful is just a test on how this will have an impact on the rank of the vector if the document is longer Free text seaRCh is a wonderful Thing'), to_tsquery('wonderful & thing'));

  --> rank = 0.0991726



## Verbesserungen

- Derzeit arbeite ich auf der VIEW search_pages und mache damit laut EXPLAIN ANALYZE keinen gebrauch des GIN INDEX, da der tsvector unified_tsv noch kein index hat

  --> mögliche verbesserung ist das erstellen einer Table oder Materialized View anstatt der normalen view





[https://de.wikipedia.org/wiki/Tf-idf-Ma%C3%9F](https://de.wikipedia.org/wiki/Tf-idf-Maß)

https://www.compose.com/articles/indexing-for-full-text-search-in-postgresql/

https://www.postgresql.org/docs/9.1/sql-createfunction.html



https://www.programcreek.com/python/example/88977/sqlalchemy.func.to_tsvector



## Fragen

- wie validiere ich meinen Algorithmus der die besten Ergebnisse zurückgeben soll