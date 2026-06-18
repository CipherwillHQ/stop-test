import express from 'express';
import { Server } from 'http';

const app = express();
const PORT = Number(process.env.PORT) || 3000;

app.get('/', (_req, res) => {
  res.send('Server is running');
});

app.get('/health', (_req, res) => {
  res.status(200).send('OK');
});

const server: Server = app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});

let shuttingDown = false;

function gracefulShutdown(signal: string) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`\nReceived ${signal}. Waiting 5 seconds before shutting down...`);

  setTimeout(() => {
    console.log('Closing server...');
    server.close((err) => {
      if (err) {
        console.error('Error during shutdown:', err);
        process.exit(1);
      }
      console.log('Server closed gracefully');
      process.exit(0);
    });

    setTimeout(() => {
      console.error('Forced shutdown after timeout');
      process.exit(1);
    }, 5000);
  }, 5000);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
