# Sorting distribuito utilizzando il paradigma MapReduce

Lo scopo del progetto era realizzare un'applicazione distribuita che risolve il problema del sorting di una sequenza di numeri interi, utilizzando il paradigma MapReduce.

Il progetto è stato realizzato utilizzando Go e RPC.

L'applicazione ha un'architettura **master-worker**.
Il nodo master 

## Eseguire l'applicazione

1. I parametri di configurazione sono nel file `config.json` che si trova nella cartella `config` ed ha la seguente struttura:

    ```json
    {
        "workers" : ["localhost:3000", "localhost:3001", "localhost:3002", "localhost:3003"],
        "master": "31415",
        "map_nodes": [1,2,3,4],
        "reduce_nodes": [1,2,3,4],
        "out_files": ["file1", "file2", "file3","file4"]
    }
    ```
    All'interno sono presenti:
    - gli indirizzi dei worker
    - l'indirizzo del nodo master
    - a quali worker è assegnato il compito di Map
    - a quali worker è assegnato il compito di Reduce
    - il nome dei file di output per ciascun Reducer

    La configurazione di default prevede 4 nodi worker totali, dove ciascun nodo è sia un Mapper che un Reducer.
    Per modificare la configurazione (# di worker totali, nodi Mapper, nodi Reducer, indirizzi, etc.) basta modificare il file prima di avviare l'esecuzione.
1. 




