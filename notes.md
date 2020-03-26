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





[https://de.wikipedia.org/wiki/Tf-idf-Ma%C3%9F](https://de.wikipedia.org/wiki/Tf-idf-Maß)

https://www.compose.com/articles/indexing-for-full-text-search-in-postgresql/

https://www.postgresql.org/docs/9.1/sql-createfunction.html



## Fragen

- wie validiere ich meinen Algorithmus der die besten Ergebnisse zurückgeben soll