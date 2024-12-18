# Sorting distribuito utilizzando il paradigma MapReduce

Lo scopo del progetto era realizzare un'applicazione distribuita che risolve il problema del sorting di una sequenza di numeri interi, utilizzando il paradigma MapReduce.

Il progetto è stato realizzato utilizzando Go e RPC.

## Eseguire l'applicazione

1. **(Opzionale) Modificare i parametri di configurazione e la sequenza di input.** 

    I parametri di configurazione sono contenuti nel file `config.json` che si trova nella cartella `config` ed ha la seguente struttura:

    ```json
    {
        "workers" : ["localhost:3000", "localhost:3001", "localhost:3002", "localhost:3003"],
        "master": "localhost:31415",
        "map_nodes": [1,2,3,4],
        "reduce_nodes": [1,2,3,4],
        "out_files": ["file1", "file2", "file3","file4"]
    }
    ```
    All'interno sono presenti:
    - gli indirizzi dei worker
    - l'indirizzo del nodo master
    - quali worker sono dei Mapper
    - quali worker sono dei Reducer
    - il nome dei file di output per ciascun Reducer

    La configurazione di default prevede 4 nodi worker totali, ciascun dei quali è sia un Mapper che un Reducer. Ovviamente, è possibile configurare tutti i parametri prima dell'esecuzione.

    La sequenza di input, invece, è contenuta nel file `input.json` nella cartella `inout_files`.
1. **Avviare i nodi Worker.** Da terminali differenti, far partire tanti nodi Worker quanti sono specificati nel file di configurazione tramite il comando:
    
    $ go run worker.go [index]

    Il parametro a linea di comando **[index]** indica l'**indice (0, 1, 2, 3, etc.) del Worker che si sta lanciando** e serve per recuperare l'indirizzo giusto sul quale quel Worker si metterà in ascolto e potrà essere contattato.
1. **Avviare il master.** Da un ulteriore terminale, far partire il nodo Master.

    $ go run master.go


## Architettura e feature principali

Per l'applicazione è stata adottata un'architettura **master-worker**.

Il nodo master si occupa di recuperare i dati di input dal file, di eseguire un campionamento di questi dati così da fissare i range di valori per ciascuno dei nodi Reducer e, infine, dopo aver partizionato i dati di input in n chunk (dove n è il numero di nodi Mapper presenti), invia ciascun chunk al rispettivo nodo Mapper.

Ogni nodo Mapper esegue il Map della porzione di dati che ha ricevuto dal master. Una volta terminato il sorting di questi dati, i dati intermedi vengono inviati dai Mapper ai nodi Reducer, partizionandoli in base ai range definiti con il campionamento.

A questo punto, i Reducer, una volta che hanno ricevuto il risultato intermedio da tutti i nodi Mapper, effettuano il sorting della sequenza totale di numeri e ciascuno scrive il proprio risultato su un file apposito.

Una volta che tutti i Reducer hanno scritto sui loro file e terminato la fase di Reduce, il master esegue un Merge dei file e ricostruisce la sequenza totale ordinata.

Per eseguire il campionamento dei dati di input, è stata utilizzata la tecnica del **campionamento istografico con adattamento dinamico**. Questa è stata implementata nella funzione `SampleInput` nel file `utils.go`.
Si tratta di una strategia che permette di partizionare in modo bilanciato i dati, basandosi sulla loro distribuzione e non fissando range fissi. Questo è particolarmente utile in caso di sistemi distribuiti, perché permette di evitare che il carico di lavoro sia sbilanciato tra i nodi e che alcuni di questi siano sovraccarichi.
L'istogramma rappresenta la distribuzione dei dati in intervalli (o "bin") di uguale larghezza. Ognuno di questi bin contiene un conteggio del numero di elementi che rientrano in quel range. 

Per la creazione dell'istogramma, si è calcolato il valore massimo e minimo all'interno del dataset e poi si è diviso l'intervallo [min,max] in un numero fissato di bin, contando per ciascuno di questi bin il numero di elementi del dataset che ricadono in esso.

A partire dall'istogramma, sono stati costruiti i range per i Reducer in modo tale che ciascuno gestisca un numero di elementi approssimativamente simile al limite massimo stabilito. Per determinare questi range, si sommano progressivamente i valori dei contatori degli elementi nei vari bin dell'istogramma fino a raggiungere il numero massimo di elementi consentito per un Reducer (bucketDim). Una volta raggiunto questo limite, il valore massimo del range per quel Reducer viene fissato. I range così calcolati vengono successivamente salvati nel file `utils/sampled.json`, da cui potranno essere letti dai Mapper nel momento in cui devono partizionare il loro risultato intermedio tra i vari Reducer.







