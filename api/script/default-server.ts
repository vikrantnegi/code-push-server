// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import * as api from "./api";
import { AWSStorage } from "./storage/aws-storage";
import { fileUploadMiddleware } from "./file-upload-manager";
import { JsonStorage } from "./storage/json-storage";
import { RedisManager } from "./redis-manager";
import { Storage } from "./storage/storage";
import { Response } from "express";
import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";

import * as bodyParser from "body-parser";
const domain = require("express-domain-middleware");
import * as express from "express";
import * as q from "q";

interface Secret {
  id: string;
  value: string;
}

function bodyParserErrorHandler(err: any, req: express.Request, res: express.Response, next: Function): void {
  if (err) {
    if (err.message === "invalid json" || (err.name === "SyntaxError" && ~err.stack.indexOf("body-parser"))) {
      req.body = null;
      next();
    } else {
      next(err);
    }
  } else {
    next();
  }
}

export function start(done: (err?: any, server?: express.Express, storage?: Storage) => void, useJsonStorage?: boolean): void {
  let storage: Storage;
  let isSecretsManagerConfigured: boolean;
  let secretsClient: SecretsManagerClient;

  q<void>(null)
    .then(async () => {
      if (useJsonStorage) {
        storage = new JsonStorage();
      } else if (!process.env.AWS_SECRETS_MANAGER_REGION) {
        storage = new AWSStorage();
      } else {
        isSecretsManagerConfigured = true;

        const region = process.env.AWS_SECRETS_MANAGER_REGION;
        secretsClient = new SecretsManagerClient({ region });

        const command = new GetSecretValueCommand({
          SecretId: `storage-${process.env.AWS_STORAGE_NAME}`
        });

        const response = await secretsClient.send(command);
        const secret = JSON.parse(response.SecretString);

        storage = new AWSStorage(
          process.env.AWS_REGION,
          secret.accessKeyId,
          secret.secretAccessKey
        );
      }
    })
    .then(() => {
      const app = express();
      const auth = api.auth({ storage: storage });
      const appInsights = api.appInsights();
      const redisManager = new RedisManager();

      // First, to wrap all requests and catch all exceptions.
      app.use(domain);

      // Monkey-patch res.send and res.setHeader to no-op after the first call
      app.use((req: express.Request, res: express.Response, next: (err?: any) => void): any => {
        const originalSend = res.send;
        const originalSetHeader = res.setHeader;
        res.setHeader = (name: string, value: string | number | readonly string[]): Response => {
          if (!res.headersSent) {
            originalSetHeader.apply(res, [name, value]);
          }
          return {} as Response;
        };

        res.send = (body: any) => {
          if (res.headersSent) {
            return res;
          }
          return originalSend.apply(res, [body]);
        };
        next();
      });

      if (process.env.LOGGING) {
        app.use((req: express.Request, res: express.Response, next: (err?: any) => void): any => {
          console.log();
          console.log(`[REST] Received ${req.method} request at ${req.originalUrl}`);
          next();
        });
      }

      // Configure middleware and routes
      app.use(api.requestTimeoutHandler());
      app.use(api.inputSanitizer());
      app.use(bodyParser.urlencoded({ extended: true }));

      const jsonOptions: any = { limit: "10kb", strict: true };
      if (process.env.LOG_INVALID_JSON_REQUESTS === "true") {
        jsonOptions.verify = (req: express.Request, res: express.Response, buf: Buffer, encoding: string) => {
          if (buf && buf.length) {
            (<any>req).rawBody = buf.toString();
          }
        };
      }

      app.use(bodyParser.json(jsonOptions));
      app.use(bodyParserErrorHandler);
      app.use(appInsights.router());

      // Basic routes
      app.get("/", (req: express.Request, res: express.Response) => {
        res.send("Welcome to the CodePush REST API!");
      });

      // Configure app settings
      app.set("etag", false);
      app.set("views", __dirname + "/views");
      app.set("view engine", "ejs");
      app.use("/auth/images/", express.static(__dirname + "/views/images"));
      app.use(api.headers({ origin: process.env.CORS_ORIGIN || "http://localhost:4000" }));
      app.use(api.health({ storage: storage, redisManager: redisManager }));

      if (process.env.DISABLE_ACQUISITION !== "true") {
        app.use(api.acquisition({ storage: storage, redisManager: redisManager }));
      }

      if (process.env.DISABLE_MANAGEMENT !== "true") {
        if (process.env.DEBUG_DISABLE_AUTH === "true") {
          app.use((req, res, next) => {
            req.user = {
              id: process.env.DEBUG_USER_ID || "default"
            };
            next();
          });
        } else {
          app.use(auth.router());
        }
        app.use(auth.authenticate, fileUploadMiddleware, api.management({ storage: storage, redisManager: redisManager }));
      } else {
        app.use(auth.legacyRouter());
      }

      app.use(appInsights.errorHandler);

      if (isSecretsManagerConfigured) {
        // Refresh credentials from AWS Secrets Manager
        setInterval(async () => {
          try {
            const command = new GetSecretValueCommand({
              SecretId: `storage-${process.env.AWS_STORAGE_NAME}`
            });
            const response = await secretsClient.send(command);
            const secret = JSON.parse(response.SecretString);

            await (<AWSStorage>storage).reinitialize(
              process.env.AWS_REGION,
              secret.accessKeyId,
              secret.secretAccessKey
            );
          } catch (error) {
            console.error("Failed to reinitialize storage from AWS Secrets Manager");
            appInsights.errorHandler(error);
          }
        }, Number(process.env.REFRESH_CREDENTIALS_INTERVAL) || 24 * 60 * 60 * 1000 /*daily*/);
      }

      done(null, app, storage);
    })
    .catch(error => {
      done(error);
    });
}
