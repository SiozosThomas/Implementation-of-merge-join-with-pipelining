# Implementation-of-merge-join-with-pipelining

Στόχος του project είναι να υλοποιήσει το merge-join σαν iterator, ο οποίος θα
μπορεί να χρησιμοποιηθεί για pipelining. Το πεδίο συνένωσης είναι το πρώτο.
Σαν παραμέτρους μπορούμε να βάλουμε και ποια άλλα πεδία θέλουμε να εμφανίζονται.

## Data

Τα δεδομένα χρησιμοποιούνται από την βάση της IMDB (Internet Movie Database).<br/>
[link](https://www.imdb.com/interfaces/)

## Running the project

Στην αρχή ζητείται ένα combo για τις παραμέτρους του project.
* scan: Όταν θέλουμε απλά να διαβάσει το αρχείο εισόδου.
* merge-join: Όταν θέλουμε να εκτελέσει merge-join σε δύο αρχεία.

Τα αρχεία εισόδου πρέπει να βρίσκονται σε φάκελο "data".<br/>
Μέσα στον φάκελο "data" τους φακέλους:
* name.basics.tsv
* title.akas.tsv
* title.basics.tsv
* title.crew.tsv
* title.episode.tsv
* title.pincipals.tsv
* title.ratings.tsv<br/>
Οι οποίοι βρίσκονται στα δεδομένα της βάσης.

## Output File

Παράδειγμα για είσοδο "scan" - "scan".<br/>
Αρχεία:
* name.basics.tsv
* title.akas.tsv<br/>
Πεδία:
* 2 2
* 2 1<br/>
![Alt Text](/output/result.png)
