# Sorting distribuito utilizzando il paradigma MapReduce

Lo scopo del progetto era realizzare un'applicazione distribuita che risolve il problema del sorting di una sequenza di numeri interi, utilizzando il paradigma MapReduce.

Il progetto è stato realizzato utilizzando Go e RPC.

L'applicazione ha un'architettura **master-worker**.
Il nodo master 

## Eseguire l'applicazione

1. I parametri di configurazione sono nel file `config.json` che si trova nella cartella `config` ed ha la seguente struttura:

    ```json
    {
        "ports" : ["3000", "3001", "3002", "3003"],
        "master": "31415",
        "map_nodes": [1,2,3,4],
        "reduce_nodes": [1,2,3,4],
        "out_files": ["file1", "file2", "file3","file4"]
    }
    ```
    

1.




