## Notes

Originally i wanted to use the python library `wikipedia`.
But this library is not able to search for pages of a certain category.
Therefore i decided to use the library `pymediawiki` instead.
`pymediawiki` is licensed under the MIT License.
Documentation: https://pymediawiki.readthedocs.io/en/latest/code.html

Problem: the results of the pymediawiki query do not 100% match with the results of the wikiepedia webpage query, most likely because of much less search parameter control



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