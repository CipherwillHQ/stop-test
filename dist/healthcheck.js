"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const http_1 = __importDefault(require("http"));
const HOST = "127.0.0.1";
const PORT = Number(process.env.PORT) || 3000;
const PATH = "/health";
const TIMEOUT_MS = 5000;
console.log(`[Health Check] Checking health at http://${HOST}:${PORT}${PATH}...`);
const requestOptions = {
    host: HOST,
    port: PORT,
    path: PATH,
    method: "GET",
};
const req = http_1.default.request(requestOptions, handleResponse);
const globalTimeout = setTimeout(() => {
    console.error(`[Health Check] Global timeout of ${TIMEOUT_MS}ms reached`);
    req.destroy();
    process.exit(1);
}, TIMEOUT_MS);
globalTimeout.unref();
function handleResponse(res) {
    let responseData = "";
    res.on("data", (chunk) => {
        responseData += chunk;
    });
    res.on("end", () => {
        clearTimeout(globalTimeout);
        const statusCode = res.statusCode || 0;
        if (statusCode === 200) {
            console.log(`[Health Check] Success with status code: ${statusCode}`);
            console.log(`[Health Check] Response: ${responseData}`);
            process.exit(0);
        }
        else {
            console.error(`[Health Check] Failed with status code: ${statusCode}`);
            console.error(`[Health Check] Response: ${responseData}`);
            process.exit(1);
        }
    });
}
req.on("error", (err) => {
    clearTimeout(globalTimeout);
    console.error(`[Health Check] Connection error: ${err.message}`);
    process.exit(1);
});
req.end();
