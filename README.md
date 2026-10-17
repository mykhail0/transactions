# transactions

## Introduction

Transaction is a sequence of operations which are either all to be executed or
none of them at all (atomicity), and that results of intermediate operations are
not visible between different transactions (isolation). This project implements
a concurrent transaction manager in Java 11 which was an assignment from classes
on concurrent programming at the University of Warsaw.

To run the demo you should first compile using [compile.sh](compile.sh) and then
`java cp1.demo.Transactions`.

## Specification

We consider a system with a certain unknown number of threads. Threads operate
on a certain collection of resources (objects inheriting after
[`cp1.base.Resource`](cp1/base/Resource.java)), each of which has a constant
unique id ([`cp1.base.ResourceId`](cp1/base/ResourceId.java)). As a result of
such operations (represented by objects implementing
[`cp1.base.ResourceId`](cp1/base/ResourceId.java) interface) the collection of
resources does not change. However, states of select resources may change.

Operations on resources are executed under transactions. In any moment a given
thread may be executing at most one transaction. Transactions are coordinated by
a transaction manager (implementing
[`cp1.base.TransactionManager`](cp1/base/TransactionManager.java) interface).

During creation (`newTM` method from
[`cp1.solution.TransactionManagerFactory`](cp1/solution/TransactionManagerFactory.java))
a transaction manager receives exclusive ownership over a collection of
resources, as long as an interface for asking current time, necessary for
managing transactions.

### Transaction start

Before a thread executes any operation on a resource, it must start (activate) a
transaction by calling transaction manager's (TM's) `startTransaction`.
Transaction start does not succeed if and only if the thread calling the method
hasn't yet finished a previous transaction. In such case
[`cp1.base.AnotherTransactionActiveException`](cp1/base/AnotherTransactionActiveException.java)
is thrown. The thread may then finish the current transaction and then try again
to start a new transaction. However, if the start succeeded, then the
transaction remains active until a moment it is finished as described below.

### Operations under a transaction

Wykonanie przez wątek operacji na zasobie odbywa się poprzez wywołanie przez niego metody operateOnResourceInCurrentTransaction MT, której argumenty oznaczają identyfikator zasobu oraz operację do wykonania. Jeśli wątek nie ma aktywnej transakcji, wywołanie ww. metody kończy się zgłoszeniem wyjątku cp1.base.NoActiveTransactionException. Analogicznie, wywołanie dla identyfikatora zasobu, który nie jest pod kontrolą MT, kończy się wyjątkiem cp1.base.UnknownResourceIdException z argumentem odpowiadającym identyfikatorowi niekontrolowanego zasobu. Wreszcie, jeśli aktywna transakcja została wcześniej anulowana, jak wyjaśniamy dalej, to wywołanie kończy się wyjątkiem cp1.base.ActiveTransactionAborted.

W przeciwnym razie wątek rozpoczyna ubieganie się o dostęp do wskazanego zasobu. Jeśli w ramach aktywnej transakcji wątek już taki dostęp uzykał poprzednio, to jest udało mu się wcześniej co najmniej raz rozpocząć wykonanie jakiejś operacji na tym samym zasobie, to dostęp jest mu przyznawany natychmiastowo. W przeciwnym przypadku sprawdzane jest, czy inna aktywna transakcja miała kiedykolwiek przyznany dostęp do zasobu. Jeśli taka transakcja nie istnieje, to dostęp zostaje przyznany rozważanej transakcji. W przeciwnym razie z kolei rozważana transakcja musi potencjalnie zaczekać na zakończenie transakcji, która uzyskała wcześniej dostęp do wskazanego zasobu.

Jednakże gdy takie oczekiwanie doprowadziłoby do zakleszczenia pewnej grupy transakcji, MT anuluje tę z transakcji w grupie, która została rozpoczęta najpóźniej. Czas rozpoczęcia mierzony jest wartością metody getTime interfejsu LocalTimeProvider (przekazanemu MT przy jego tworzeniu), a w przypadku, gdy kilka transakcji ma ten sam czas rozpoczęcia, decydują identyfikatory wykonujących je wątków (metoda getId klasy Thread), to jest transakcja wątku z największym identyfikatorem uważana jest za najpóźniejszą spośród transakcji z tym samym czasem rozpoczęcia. Anulowanie transakcji wymaga także przerwania wątku, który ją rozpoczął (metoda interrupt klasy Thread).

Anulowana transakcja nie może być kontynuowana, to jest można jedynie ją zakończyć, jak wyjaśniamy dalej. W szczególności, jak wspomniano wcześniej, próba wykonania jakiejkolwiek operacji w ramach tej transakcji kończy się wyjątkiem cp1.base.ActiveTransactionAborted.

Gdy wątek uzyska już dostęp do zasobu, MT rozpoczyna wykonywanie na tym zasobie operacji przekazanej jako argument funkcji operateOnResourceInCurrentTransaction poprzez wywołanie na obiekcie tej operacji metody execute interfejsu cp1.base.ResourceOperation z argumentem odpowiadającym zasobowi. Wykonanie operacji musi się odbyć w kontekście wątku, który tę operację zlecił, to jest żaden wątek nie może wykonać ww. metody execute na obiekcie operacji przekazanej MT przez inny wątek.

Wykonanie operacji może zakończyć się wyjątkiem cp1.base.ResourceOperationException, który musi zostać przekazany przez MT do wołającego wątku. W takim przypadku mówimy, że operacja kończy się niepowodzeniem. Metoda execute operacji kończącej się niepowodzeniem gwarantuje, że zasób pozostaje w nienaruszonym stanie, to jest stanie sprzed wykonania tej operacji.

Operacja, której wykonanie nie spowoduje wyjątku cp1.base.ResourceOperationException kończy się natomiast sukcesem. Taka operacja może zmodyfikować stan zasobu.

Niezależnie od tego, jak się zakończy, dana operacja może być przez wątek wykonywana wiele razy, na tych samych lub różnych zasobach. Co więcej, różne wykonania tej samej operacji mogą się różnie kończyć.

Należy także zauważyć, iż w trakcie oczekiwania na zasób lub wykonywania na nim operacji, wątek może zostać przerwany (poprzez wywołanie metody interrupt z klasy Thread). W takim przypadku wywołanie metody operateOnResourceInCurrentTransaction MT kończy się wyjątkiem InterruptedException, pozostawiając bez zmian zarówno stan zasobu, jak i transakcji, przy czym w zależności od momentu, w którym pojawiło się przerwanie, transakcja może dostać lub nie dostęp do zasobu. Dokładniej, jeśli przerwanie pojawiło się przed lub w trakcie przyznawania dostępu, to transakcja tego dostępu nie uzyskuje. W przeciwnym przypadku dostęp zostaje przyznany.
Kończenie transakcji

Wątek może zakończyć aktywną transakcję na jeden z dwóch sposobów:

    poprzez zatwierdzenie (wywołanie metody commitCurrentTransaction MT) albo
    poprzez cofnięcie (wywołanie metody rollbackCurrentTransaction MT).

Zatwierdzenie transakcji powoduje, że wszelkie zmiany zasobów MT wykonane przez operacje w ramach tej transakcji stają się widoczne dla innych transakcji. Zatwierdzenie transakcji przez wątek nie udaje się tylko w dwóch przypadkach. Po pierwsze, gdy wątek nie ma aktywnej transakcji, zgłaszany jest wyjątek cp1.base.NoActiveTransactionException. Po drugie, gdy aktywna transakcja wątku została wcześniej anulowana, zgłaszany jest wyjątek cp1.base.ActiveTransactionAborted a transakcja nie jest kończona.

Cofnięcie transakcji powoduje z kolei, że wszelkie zmiany zasobów MT wykonane przez operacje w ramach tej transakcji zostają wycofane, to jest stan tych zasobów jest taki sam, jak w momencie rozpoczynania transakcji. Cofnięcie transakcji przez wątek zawsze się udaje. W szczególności, gdy wątek nie ma aktywnej transakcji, jest to po prostu operacja pusta.

Wycofanie zmian wykonanych przez daną operację na danym zasobie odbywa się poprzez wywołanie na obiekcie tej operacji metody undo interfejsu cp1.base.ResourceOperation z argumentem odpowiadającym zasobowi. Jak w przypadku metody execute, wywołanie metody undo dla operacji musi odbyć się w kontekście wątku, który tę operację zlecił. W przeciwieństwie do metody execute, metoda undo nie zgłasza żadnych wyjątków. Wycofywane są jedynie operacje zakończone sukcesem i robione jest to w odwrotnej kolejności niż kolejność ich wykonania w ramach transakcji.

Zakończenie transakcji na którykolwiek z dwóch możliwych sposobów powoduje, że MT przestaje przechowywać jakiekolwiek informacje związane z tą transakcją.
Sprawdzanie stanu transakcji

MT udostępnia jeszcze dwie metody pozwalające wątkowi sprawdzić stan swojej transakcji. Metoda isTransactionActive, wywołana w kontekście wątku, zwraca true wtedy i tylko wtedy, gdy wątek ten ma aktywną transakcję. Metoda isTransactionAborted z kolei zwraca true wtedy i tylko wtedy, gdy wątek ma aktywną transakcję, lecz została ona anulowana.
