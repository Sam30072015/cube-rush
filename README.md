# Cube Rush Server

Ein kleiner Server für dein Cube-Rush-Spiel.

## Start

1. Node.js installieren.
2. In diesem Ordner ausführen:

```bash
npm install
npm start
```

Dann im Browser öffnen:

`http://localhost:3000`

## Admin

Standardmäßig ist der Admin-Key `4729`.

Für einen echten Online-Server solltest du den Key über eine Umgebungsvariable setzen:

```bash
ADMIN_KEY=DEIN_GEHEIMER_KEY npm start
```

## Enthalten

- Online-Spieler-Verbindung per WebSocket
- Admin kann einem **online** befindlichen Spieler Münzen/Skins geben
- Admin kann Server-Nachrichten an alle verbundenen Spieler senden
- Minuten + Sekunden für Server-Nachrichten
- Statische Auslieferung der Cube-Rush-HTML
