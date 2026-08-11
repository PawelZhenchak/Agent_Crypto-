# Architektura T4-only

```mermaid
flowchart TD
    A["Plus500 Futures T4 API"] --> B["Odizolowany worker .NET"]
    B --> C["Loopback market-data bridge"]
    C --> D["Agent Python read-only"]
    D --> E["Risk gate"]
    E --> F["ALERT lub NO_SIGNAL"]
    D --> G["PostgreSQL 16"]
```

Worker .NET ma połączenie SSL do T4 i prywatne dane sesji. Agent Python widzi
jedynie znormalizowane świece, cenę referencyjną i metadane atestacji. Endpointy
zleceń nie mogą być wystawione przez most.

Most jest dostępny tylko przez loopback. Adapter odrzuca zdalny host, credentials
w URL, redirect, błędny source ID, niepełną historię i nie-UTC timestamps.

PostgreSQL rozdziela historię migracji od aktywnej polityki. `t4_runtime_config`
jest append-only i jednoznacznie wymusza wyłączone order routes.
