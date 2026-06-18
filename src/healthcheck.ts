import http from "http";

const HOST = "127.0.0.1";
const PORT = Number(process.env.PORT) || 3000;
const PATH = "/health";
const TIMEOUT_MS = 5000;

console.log(`[Health Check] Checking health at http://${HOST}:${PORT}${PATH}...`);

const requestOptions: http.RequestOptions = {
  host: HOST,
  port: PORT,
  path: PATH,
  method: "GET",
};

const req = http.request(requestOptions, handleResponse);

const globalTimeout = setTimeout(() => {
  console.error(`[Health Check] Global timeout of ${TIMEOUT_MS}ms reached`);
  req.destroy();
  process.exit(1);
}, TIMEOUT_MS);
globalTimeout.unref();

function handleResponse(res: http.IncomingMessage) {
  let responseData = "";

  res.on("data", (chunk: Buffer) => {
    responseData += chunk;
  });

  res.on("end", () => {
    clearTimeout(globalTimeout);
    const statusCode = res.statusCode || 0;
    if (statusCode === 200) {
      console.log(`[Health Check] Success with status code: ${statusCode}`);
      console.log(`[Health Check] Response: ${responseData}`);
      process.exit(0);
    } else {
      console.error(`[Health Check] Failed with status code: ${statusCode}`);
      console.error(`[Health Check] Response: ${responseData}`);
      process.exit(1);
    }
  });
}

req.on("error", (err: NodeJS.ErrnoException) => {
  clearTimeout(globalTimeout);
  console.error(`[Health Check] Connection error: ${err.message}`);
  process.exit(1);
});

req.end();
