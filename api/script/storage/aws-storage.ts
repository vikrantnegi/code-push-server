// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import * as q from "q";
import * as shortid from "shortid";
import * as stream from "stream";
import * as storage from "./storage";
import * as utils from "../utils/common";
import { Readable } from 'stream';

import {
  DynamoDBClient,
  PutItemCommand,
  GetItemCommand,
  DeleteItemCommand,
  UpdateItemCommand,
  QueryCommand,
  TransactWriteItemsCommand,
  BatchWriteItemCommand,
  AttributeValue
} from "@aws-sdk/client-dynamodb";
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  CreateBucketCommand,
  HeadBucketCommand
} from "@aws-sdk/client-s3";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { isPrototypePollutionKey } from "./storage";

interface Pointer {
  partitionKey: string;
  sortKey: string;
}

interface DeploymentKeyPointer {
  appId: string;
  deploymentId: string;
  accountId: string;  // Add this line
}

interface AccessKeyPointer {
  accountId: string;
  expires: number;
}

module Keys {
  const DELIMITER = "#";  // Changed from space for DynamoDB compatibility
  const LEAF_MARKER = "*";

  export function getAccountPartitionKey(accountId: string): string {
    validateParameters(Array.prototype.slice.apply(arguments));
    return "accountId" + DELIMITER + accountId;
  }

  export function getAccountAddress(accountId: string): Pointer {
    validateParameters(Array.prototype.slice.apply(arguments));
    return <Pointer>{
      partitionKey: getAccountPartitionKey(accountId),
      sortKey: getHierarchicalAccountRowKey(accountId),
    };
  }

  export function getAppPartitionKey(appId: string): string {
    validateParameters(Array.prototype.slice.apply(arguments));
    return "appId" + DELIMITER + appId;
  }

  export function getHierarchicalAppRowKey(appId?: string, deploymentId?: string): string {
    validateParameters(Array.prototype.slice.apply(arguments));
    return generateHierarchicalAppKey(/*markLeaf=*/ true, appId, deploymentId);
  }

  export function getHierarchicalAccountRowKey(accountId: string, appId?: string): string {
    validateParameters(Array.prototype.slice.apply(arguments));
    return generateHierarchicalAccountKey(/*markLeaf=*/ true, accountId, appId);
  }

  export function generateHierarchicalAppKey(markLeaf: boolean, appId: string, deploymentId?: string): string {
    validateParameters(Array.prototype.slice.apply(arguments).slice(1));
    let key = delimit("appId", appId, /*prependDelimiter=*/ false);

    if (typeof deploymentId !== "undefined") {
      key += delimit("deploymentId", deploymentId);
    }

    if (markLeaf) {
      const lastIdDelimiter: number = key.lastIndexOf(DELIMITER);
      key = key.substring(0, lastIdDelimiter) + LEAF_MARKER + key.substring(lastIdDelimiter);
    }

    return key;
  }

  export function generateHierarchicalAccountKey(markLeaf: boolean, accountId: string, appId?: string): string {
    validateParameters(Array.prototype.slice.apply(arguments).slice(1));
    let key = delimit("accountId", accountId, /*prependDelimiter=*/ false);

    if (typeof appId !== "undefined") {
      key += delimit("appId", appId);
    }

    if (markLeaf) {
      const lastIdDelimiter: number = key.lastIndexOf(DELIMITER);
      key = key.substring(0, lastIdDelimiter) + LEAF_MARKER + key.substring(lastIdDelimiter);
    }

    return key;
  }

  export function getAccessKeyRowKey(accountId: string, accessKeyId?: string): string {
    validateParameters(Array.prototype.slice.apply(arguments));
    let key: string = "accountId_" + accountId + "_accessKeyId*_";

    if (accessKeyId !== undefined) {
      key += accessKeyId;
    }

    return key;
  }

  export function isDeployment(sortKey: string): boolean {
    return sortKey.indexOf("deploymentId*") !== -1;
  }

  export function getEmailShortcutAddress(email: string): Pointer {
    validateParameters(Array.prototype.slice.apply(arguments));
    return <Pointer>{
      partitionKey: "email" + DELIMITER + email.toLowerCase(),
      sortKey: "",
    };
  }

  export function getShortcutDeploymentKeyPartitionKey(deploymentKey: string): string {
    validateParameters(Array.prototype.slice.apply(arguments));
    return delimit("deploymentKey", deploymentKey, /*prependDelimiter=*/ false);
  }

  export function getShortcutDeploymentKeyRowKey(): string {
    return "";
  }

  export function getShortcutAccessKeyPartitionKey(accessKeyName: string, hash: boolean = true): string {
    validateParameters(Array.prototype.slice.apply(arguments));
    return delimit("accessKey", hash ? utils.hashWithSHA256(accessKeyName) : accessKeyName, /*prependDelimiter=*/ false);
  }

  function validateParameters(parameters: string[]): void {
    parameters.forEach((parameter: string): void => {
      if (parameter && (parameter.indexOf(DELIMITER) >= 0 || parameter.indexOf(LEAF_MARKER) >= 0)) {
        throw storage.storageError(storage.ErrorCode.Invalid, `The parameter '${parameter}' contained invalid characters.`);
      }
    });
  }

  function delimit(fieldName: string, value: string, prependDelimiter = true): string {
    const prefix = prependDelimiter ? DELIMITER : "";
    return prefix + fieldName + DELIMITER + value;
  }
}

export class AWSStorage implements storage.Storage {
  public static NO_ID_ERROR = "No id set";

  private static HISTORY_BUCKET_NAME = "package-history-v1";
  private static MAX_PACKAGE_HISTORY_LENGTH = 50;
  private static TABLE_NAME = "storage-v2";

  private _dynamoClient: DynamoDBClient;
  private _s3Client: S3Client;
  private _setupPromise: q.Promise<void>;

  public constructor(region?: string, accessKeyId?: string, secretAccessKey?: string) {
    shortid.characters("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-");
    this._setupPromise = this.setup(region, accessKeyId, secretAccessKey);
  }

  public reinitialize(region?: string, accessKeyId?: string, secretAccessKey?: string): q.Promise<void> {
    console.log("Re-initializing AWS storage");
    return this.setup(region, accessKeyId, secretAccessKey);
  }

  private setup(region?: string, accessKeyId?: string, secretAccessKey?: string): q.Promise<void> {
    const config = {
      region: region || process.env.AWS_REGION || 'us-east-1',
      credentials: {
        accessKeyId: accessKeyId || process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: secretAccessKey || process.env.AWS_SECRET_ACCESS_KEY
      }
    };

    if (!config.credentials.accessKeyId || !config.credentials.secretAccessKey) {
      throw new Error("AWS credentials not set");
    }

    this._dynamoClient = new DynamoDBClient(config);
    this._s3Client = new S3Client(config);

    return q(this.createResources());
  }

  private async createResources(): Promise<void> {
    try {
      // Create S3 buckets if they don't exist
      await this._s3Client.send(new CreateBucketCommand({
        Bucket: AWSStorage.TABLE_NAME
      }));

      await this._s3Client.send(new CreateBucketCommand({
        Bucket: AWSStorage.HISTORY_BUCKET_NAME
      }));

      // Create health check items
      await this._s3Client.send(new PutObjectCommand({
        Bucket: AWSStorage.TABLE_NAME,
        Key: 'health',
        Body: 'health'
      }));

      await this._s3Client.send(new PutObjectCommand({
        Bucket: AWSStorage.HISTORY_BUCKET_NAME,
        Key: 'health',
        Body: 'health'
      }));

      // Create DynamoDB health check item
      await this._dynamoClient.send(new PutItemCommand({
        TableName: AWSStorage.TABLE_NAME,
        Item: marshall({
          partitionKey: 'health',
          sortKey: 'health',
          health: 'health'
        })
      }));

      return q();
    } catch (error) {
      if (error.name === 'ResourceInUseException' || error.name === 'BucketAlreadyOwnedByYou') {
        return q();
      }
      throw error;
    }
  }

  public checkHealth(): q.Promise<void> {
    return this._setupPromise
      .then(() => {
        const dynamoCheck = this.dynamoHealthCheck();
        const mainBucketCheck = this.s3HealthCheck(AWSStorage.TABLE_NAME);
        const historyBucketCheck = this.s3HealthCheck(AWSStorage.HISTORY_BUCKET_NAME);
        return q.all([dynamoCheck, mainBucketCheck, historyBucketCheck]);
      })
      .then(() => {
        return;
      });
  }

  private dynamoHealthCheck(): q.Promise<void> {
    return q.Promise<void>((resolve, reject) => {
      this._dynamoClient.send(new GetItemCommand({
        TableName: AWSStorage.TABLE_NAME,
        Key: marshall({
          partitionKey: 'health',
          sortKey: 'health'
        })
      }))
        .then(response => {
          if (!response.Item || unmarshall(response.Item).health !== 'health') {
            reject(storage.storageError(
              storage.ErrorCode.ConnectionFailed,
              "The DynamoDB service failed the health check"
            ));
          } else {
            resolve();
          }
        })
        .catch(reject);
    });
  }

  private s3HealthCheck(bucket: string): q.Promise<void> {
    return q.Promise<void>((resolve, reject) => {
      this._s3Client.send(new GetObjectCommand({
        Bucket: bucket,
        Key: 'health'
      }))
        .then(async response => {
          const bodyContents = await response.Body.transformToString();
          if (bodyContents !== 'health') {
            reject(storage.storageError(
              storage.ErrorCode.ConnectionFailed,
              `The S3 service failed the health check for ${bucket}`
            ));
          } else {
            resolve();
          }
        })
        .catch(reject);
    });
  }

  private static awsErrorHandler(error: any, overrideMessage: boolean = false, overrideCondition?: string, overrideValue?: string): any {
    let errorCode: storage.ErrorCode;
    let errorMessage = error.message;

    if (overrideMessage && error.name === overrideCondition) {
      errorMessage = overrideValue;
    }

    switch (error.name) {
      case 'ResourceNotFoundException':
      case 'NoSuchKey':
        errorCode = storage.ErrorCode.NotFound;
        break;
      case 'ResourceInUseException':
      case 'ConditionalCheckFailedException':
        errorCode = storage.ErrorCode.AlreadyExists;
        break;
      case 'EntityTooLarge':
      case 'ItemSizeLimitExceededException':
        errorCode = storage.ErrorCode.TooLarge;
        break;
      case 'RequestTimeout':
      case 'TimeoutError':
        errorCode = storage.ErrorCode.ConnectionFailed;
        break;
      default:
        errorCode = storage.ErrorCode.Other;
        break;
    }

    throw storage.storageError(errorCode, errorMessage);
  }

  public clearPackageHistory(accountId: string, appId: string, deploymentId: string): q.Promise<void> {
    return this._setupPromise
      .then(async () => {
        const partitionKey = Keys.getAppPartitionKey(appId);
        const sortKey = Keys.getHierarchicalAppRowKey(appId, deploymentId);

        await this._dynamoClient.send(new UpdateItemCommand({
          TableName: AWSStorage.TABLE_NAME,
          Key: marshall({
            partitionKey,
            sortKey
          }),
          UpdateExpression: 'REMOVE #package',
          ExpressionAttributeNames: {
            '#package': 'package'
          }
        }));
      });
  }

  public addApp(accountId: string, app: storage.App): q.Promise<storage.App> {
    app = storage.clone(app);
    app.id = shortid.generate();

    return this._setupPromise
      .then(() => {
        return this.getAccount(accountId);
      })
      .then((account: storage.Account) => {
        const collabMap: storage.CollaboratorMap = {};
        collabMap[account.email] = { accountId: accountId, permission: storage.Permissions.Owner };
        app.collaborators = collabMap;

        const flatApp = AWSStorage.flattenApp(app, /*updateCollaborator*/ true);
        return this.insertByAppHierarchy(flatApp, app.id);
      })
      .then(() => {
        return this.addAppPointer(accountId, app.id);
      })
      .then(() => app)
      .catch(AWSStorage.awsErrorHandler);
  }

  public getApps(accountId: string): q.Promise<storage.App[]> {
    return this._setupPromise
      .then(() => {
        return this.getCollectionByHierarchy(accountId);
      })
      .then((flatApps: any[]) => {
        return flatApps.map(flatApp => AWSStorage.unflattenApp(flatApp, accountId));
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public getApp(accountId: string, appId: string, keepCollaboratorIds: boolean = false): q.Promise<storage.App> {
    return this._setupPromise
      .then(() => {
        return this.retrieveByAppHierarchy(appId);
      })
      .then((flatApp: any) => {
        return AWSStorage.unflattenApp(flatApp, accountId);
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public updateApp(accountId: string, app: storage.App): q.Promise<void> {
    if (!app.id) throw new Error(AWSStorage.NO_ID_ERROR);

    return this._setupPromise
      .then(() => {
        return this.updateAppWithPermission(accountId, app, /*updateCollaborator*/ false);
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public addDeployment(accountId: string, appId: string, deployment: storage.Deployment): q.Promise<string> {
    let deploymentId: string;
    return this._setupPromise
      .then(() => {
        const flatDeployment = AWSStorage.flattenDeployment(deployment);
        flatDeployment.id = shortid.generate();
        deploymentId = flatDeployment.id;

        return this.insertByAppHierarchy(flatDeployment, appId, deploymentId);
      })
      .then(() => {
        return this.uploadToHistoryBlob(deploymentId, JSON.stringify([]));
      })
      .then(() => {
        const params = {
          TableName: AWSStorage.TABLE_NAME,
          Item: marshall({
            partitionKey: Keys.getShortcutDeploymentKeyPartitionKey(deployment.key),
            sortKey: Keys.getShortcutDeploymentKeyRowKey(),
            appId: appId,
            deploymentId: deploymentId
          })
        };

        return this._dynamoClient.send(new PutItemCommand(params));
      })
      .then(() => deploymentId)
      .catch(AWSStorage.awsErrorHandler);
  }

  public getDeployments(accountId: string, appId: string): q.Promise<storage.Deployment[]> {
    return this._setupPromise
      .then(() => {
        return this.getCollectionByHierarchy(accountId, appId);
      })
      .then((flatDeployments: any[]) => {
        return flatDeployments.map(flatDeployment => AWSStorage.unflattenDeployment(flatDeployment));
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public commitPackage(accountId: string, appId: string, deploymentId: string, appPackage: storage.Package): q.Promise<storage.Package> {
    if (!deploymentId) throw new Error("No deployment id");
    if (!appPackage) throw new Error("No package specified");

    appPackage = storage.clone(appPackage);
    let packageHistory: storage.Package[];

    return this._setupPromise
      .then(() => {
        return this.getPackageHistoryFromBlob(deploymentId);
      })
      .then((history: storage.Package[]) => {
        packageHistory = history;
        appPackage.label = this.getNextLabel(packageHistory);
        return this.getAccount(accountId);
      })
      .then((account: storage.Account) => {
        appPackage.releasedBy = account.email;

        const lastPackage = packageHistory && packageHistory.length ?
          packageHistory[packageHistory.length - 1] : null;
        if (lastPackage) {
          lastPackage.rollout = null;
        }

        packageHistory.push(appPackage);

        if (packageHistory.length > AWSStorage.MAX_PACKAGE_HISTORY_LENGTH) {
          packageHistory.splice(0, packageHistory.length - AWSStorage.MAX_PACKAGE_HISTORY_LENGTH);
        }

        const params = {
          TableName: AWSStorage.TABLE_NAME,
          Key: marshall({
            partitionKey: Keys.getAppPartitionKey(appId),
            sortKey: Keys.getHierarchicalAppRowKey(appId, deploymentId)
          }),
          UpdateExpression: 'SET #package = :package',
          ExpressionAttributeNames: {
            '#package': 'package'
          },
          ExpressionAttributeValues: marshall({
            ':package': JSON.stringify(appPackage)
          })
        };

        return this._dynamoClient.send(new UpdateItemCommand(params));
      })
      .then(() => {
        return this.uploadToHistoryBlob(deploymentId, JSON.stringify(packageHistory));
      })
      .then(() => appPackage)
      .catch(AWSStorage.awsErrorHandler);
  }

  private async uploadToHistoryBlob(blobId: string, content: string): Promise<void> {
    try {
      await this._s3Client.send(new PutObjectCommand({
        Bucket: AWSStorage.HISTORY_BUCKET_NAME,
        Key: blobId,
        Body: content
      }));
    } catch (error) {
      throw AWSStorage.awsErrorHandler(error);
    }
  }

  private async getPackageHistoryFromBlob(blobId: string): Promise<storage.Package[]> {
    try {
      const response = await this._s3Client.send(new GetObjectCommand({
        Bucket: AWSStorage.HISTORY_BUCKET_NAME,
        Key: blobId
      }));

      const bodyContents = await response.Body.transformToString();
      return JSON.parse(bodyContents);
    } catch (error) {
      if (error.name === 'NoSuchKey') {
        return [];
      }
      throw AWSStorage.awsErrorHandler(error);
    }
  }

  private async insertByAppHierarchy(jsObject: Object, appId: string, deploymentId?: string): Promise<string> {
    const leafId = arguments[arguments.length - 1];
    const partitionKey = Keys.getAppPartitionKey(appId);
    const sortKey = Keys.getHierarchicalAppRowKey(appId, deploymentId);

    try {
      await this._dynamoClient.send(new PutItemCommand({
        TableName: AWSStorage.TABLE_NAME,
        Item: marshall(this.wrap(jsObject, partitionKey, sortKey))
      }));

      return leafId;
    } catch (error) {
      throw AWSStorage.awsErrorHandler(error);
    }
  }

  private static flattenApp(app: storage.App, updateCollaborator: boolean = false): any {
    if (!app) return app;

    const flatApp: any = { ...app };
    if (updateCollaborator) {
      AWSStorage.deleteIsCurrentAccountProperty(app.collaborators);
      flatApp.collaborators = JSON.stringify(app.collaborators);
    }

    return flatApp;
  }

  private static unflattenApp(flatApp: any, currentAccountId: string): storage.App {
    if (!flatApp) return flatApp;

    const app = { ...flatApp };
    app.collaborators = app.collaborators ? JSON.parse(app.collaborators) : {};

    const currentUserEmail = AWSStorage.getEmailForAccountId(app.collaborators, currentAccountId);
    if (currentUserEmail && app.collaborators[currentUserEmail]) {
      app.collaborators[currentUserEmail].isCurrentAccount = true;
    }

    return app;
  }

  private static flattenDeployment(deployment: storage.Deployment): any {
    if (!deployment) return deployment;

    const flatDeployment: any = { ...deployment };
    delete flatDeployment.package;
    return flatDeployment;
  }

  private static unflattenDeployment(flatDeployment: any): storage.Deployment {
    if (!flatDeployment) return flatDeployment;

    const deployment = { ...flatDeployment };
    delete deployment.packageHistory;
    deployment.package = deployment.package ? JSON.parse(deployment.package) : null;

    return deployment;
  }

  private getNextLabel(packageHistory: storage.Package[]): string {
    if (packageHistory.length === 0) {
      return "v1";
    }

    const lastLabel = packageHistory[packageHistory.length - 1].label;
    const lastVersion = parseInt(lastLabel.substring(1));
    return "v" + (lastVersion + 1);
  }

  public getAccessKeyFromId(accountId: string, accessKeyId: string): q.Promise<storage.AccessKey> {
    const partitionKey = Keys.getAccountPartitionKey(accountId);
    const sortKey = Keys.getAccessKeyRowKey(accountId, accessKeyId);

    return this._setupPromise
      .then(async () => {
        const response = await this._dynamoClient.send(new GetItemCommand({
          TableName: AWSStorage.TABLE_NAME,
          Key: marshall({ partitionKey, sortKey })
        }));

        if (!response.Item) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }

        return this.unwrap(response.Item) as storage.AccessKey;
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public getAccountIdFromAccessKey(accessKey: string): q.Promise<string> {
    const partitionKey = Keys.getShortcutAccessKeyPartitionKey(accessKey);
    const sortKey = "";

    return this._setupPromise
      .then(async () => {
        const response = await this._dynamoClient.send(new GetItemCommand({
          TableName: AWSStorage.TABLE_NAME,
          Key: marshall({ partitionKey, sortKey })
        }));

        if (!response.Item) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }

        const accountIdObject = unmarshall(response.Item) as AccessKeyPointer;
        if (new Date().getTime() >= accountIdObject.expires) {
          throw storage.storageError(storage.ErrorCode.Expired, "The access key has expired.");
        }

        return accountIdObject.accountId;
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public addCollaborator(accountId: string, appId: string, email: string): q.Promise<void> {
    return this._setupPromise
      .then(() => {
        const getAppPromise = this.getApp(accountId, appId, /*keepCollaboratorIds*/ true);
        const accountPromise = this.getAccountByEmail(email);
        return q.all<any>([getAppPromise, accountPromise]);
      })
      .spread((app: storage.App, account: storage.Account) => {
        email = account.email; // Use original email for consistent casing
        return this.addCollaboratorWithPermissions(accountId, app, email, {
          accountId: account.id,
          permission: storage.Permissions.Collaborator
        });
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public getCollaborators(accountId: string, appId: string): q.Promise<storage.CollaboratorMap> {
    return this._setupPromise
      .then(() => {
        return this.getApp(accountId, appId, /*keepCollaboratorIds*/ false);
      })
      .then((app: storage.App) => {
        return q<storage.CollaboratorMap>(app.collaborators);
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public removeCollaborator(accountId: string, appId: string, email: string): q.Promise<void> {
    return this._setupPromise
      .then(() => {
        return this.getApp(accountId, appId, /*keepCollaboratorIds*/ true);
      })
      .then((app: storage.App) => {
        const removedCollabProperties = app.collaborators[email];

        if (!removedCollabProperties) {
          throw storage.storageError(storage.ErrorCode.NotFound, "The given email is not a collaborator for this app.");
        }

        if (!AWSStorage.isOwner(app.collaborators, email)) {
          delete app.collaborators[email];
        } else {
          throw storage.storageError(storage.ErrorCode.AlreadyExists, "Cannot remove the owner of the app from collaborator list.");
        }

        return this.updateAppWithPermission(accountId, app, /*updateCollaborator*/ true)
          .then(() => {
            return this.removeAppPointer(removedCollabProperties.accountId, app.id);
          });
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  private async getCollectionByHierarchy(accountId: string, appId?: string): Promise<any[]> {
    let partitionKey: string;
    let sortKeyPrefix: string;

    if (appId) {
      partitionKey = Keys.getAppPartitionKey(appId);
      sortKeyPrefix = Keys.generateHierarchicalAppKey(false, appId);
    } else {
      partitionKey = Keys.getAccountPartitionKey(accountId);
      sortKeyPrefix = Keys.generateHierarchicalAccountKey(false, accountId);
    }

    try {
      const response = await this._dynamoClient.send(new QueryCommand({
        TableName: AWSStorage.TABLE_NAME,
        KeyConditionExpression: 'partitionKey = :pk AND begins_with(sortKey, :sk)',
        ExpressionAttributeValues: marshall({
          ':pk': partitionKey,
          ':sk': sortKeyPrefix
        })
      }));

      if (!response.Items || response.Items.length === 0) {
        throw storage.storageError(storage.ErrorCode.NotFound);
      }

      return response.Items.map(item => this.unwrap(item));
    } catch (error) {
      throw AWSStorage.awsErrorHandler(error);
    }
  }

  private async retrieveByAppHierarchy(appId: string, deploymentId?: string): Promise<any> {
    const partitionKey = Keys.getAppPartitionKey(appId);
    const sortKey = Keys.getHierarchicalAppRowKey(appId, deploymentId);

    try {
      const response = await this._dynamoClient.send(new GetItemCommand({
        TableName: AWSStorage.TABLE_NAME,
        Key: marshall({ partitionKey, sortKey })
      }));

      if (!response.Item) {
        throw storage.storageError(storage.ErrorCode.NotFound);
      }

      return this.unwrap(response.Item);
    } catch (error) {
      throw AWSStorage.awsErrorHandler(error);
    }
  }

  private static isOwner(collaboratorsMap: storage.CollaboratorMap, email: string): boolean {
    return collaboratorsMap?.[email]?.permission === storage.Permissions.Owner;
  }

  private static isCollaborator(collaboratorsMap: storage.CollaboratorMap, email: string): boolean {
    return collaboratorsMap?.[email]?.permission === storage.Permissions.Collaborator;
  }

  private static deleteIsCurrentAccountProperty(map: storage.CollaboratorMap): void {
    if (map) {
      Object.keys(map).forEach((key: string) => {
        delete map[key].isCurrentAccount;
      });
    }
  }

  private static getEmailForAccountId(collaboratorsMap: storage.CollaboratorMap, accountId: string): string {
    if (collaboratorsMap) {
      for (const [email, props] of Object.entries(collaboratorsMap)) {
        if (props.accountId === accountId) {
          return email;
        }
      }
    }
    return null;
  }

  public getAccountByEmail(email: string): q.Promise<storage.Account> {
    const address = Keys.getEmailShortcutAddress(email);

    return this._setupPromise
      .then(async () => {
        const response = await this._dynamoClient.send(new GetItemCommand({
          TableName: AWSStorage.TABLE_NAME,
          Key: marshall({
            partitionKey: address.partitionKey,
            sortKey: address.sortKey
          })
        }));

        if (!response.Item) {
          throw storage.storageError(
            storage.ErrorCode.NotFound,
            "The specified e-mail address doesn't represent a registered user"
          );
        }

        return this.unwrap(response.Item) as storage.Account;
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  private addCollaboratorWithPermissions(accountId: string, app: storage.App, email: string, collabProps: storage.CollaboratorProperties): q.Promise<void> {
    AWSStorage.addToCollaborators(app.collaborators, email, collabProps);
    return this.updateAppWithPermission(accountId, app, /*updateCollaborator*/ true)
      .then(() => {
        return this.addAppPointer(collabProps.accountId, app.id);
      });
  }

  private async addAppPointer(accountId: string, appId: string): Promise<void> {
    const partitionKey = Keys.getAccountPartitionKey(accountId);
    const sortKey = Keys.getHierarchicalAccountRowKey(accountId, appId);

    await this._dynamoClient.send(new PutItemCommand({
      TableName: AWSStorage.TABLE_NAME,
      Item: marshall({
        partitionKey,
        sortKey,
        appId
      })
    }));
  }

  private updateAppWithPermission(accountId: string, app: storage.App, updateCollaborator: boolean): q.Promise<void> {
    const flatApp = AWSStorage.flattenApp(app, updateCollaborator);
    const partitionKey = Keys.getAppPartitionKey(app.id);
    const sortKey = Keys.getHierarchicalAppRowKey(app.id);

    return this._setupPromise
      .then(async () => {
        await this._dynamoClient.send(new PutItemCommand({
          TableName: AWSStorage.TABLE_NAME,
          Item: marshall(this.wrap(flatApp, partitionKey, sortKey))
        }));
      });
  }

  private static addToCollaborators(collaboratorsMap: storage.CollaboratorMap, email: string, collabProps: storage.CollaboratorProperties): void {
    if (collaboratorsMap && email && !isPrototypePollutionKey(email) && !collaboratorsMap[email]) {
      collaboratorsMap[email] = collabProps;
    }
  }

  private unwrap(item: { [key: string]: AttributeValue }): any {
    const unwrapped = unmarshall(item);
    const { partitionKey, sortKey, ...rest } = unwrapped;
    return rest;
  }

  private wrap(jsObject: any, partitionKey: string, sortKey: string): any {
    return {
      partitionKey,
      sortKey,
      ...jsObject
    };
  }

  public getAccount(accountId: string): q.Promise<storage.Account> {
    const address = Keys.getAccountAddress(accountId);

    return this._setupPromise
      .then(async () => {
        const response = await this._dynamoClient.send(new GetItemCommand({
          TableName: AWSStorage.TABLE_NAME,
          Key: marshall({
            partitionKey: address.partitionKey,
            sortKey: address.sortKey
          })
        }));

        if (!response.Item) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }

        return this.unwrap(response.Item) as storage.Account;
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  private async removeAppPointer(accountId: string, appId: string): Promise<void> {
    const partitionKey = Keys.getAccountPartitionKey(accountId);
    const sortKey = Keys.getHierarchicalAccountRowKey(accountId, appId);

    await this._dynamoClient.send(new DeleteItemCommand({
      TableName: AWSStorage.TABLE_NAME,
      Key: marshall({
        partitionKey,
        sortKey
      })
    }));
  }

  public addAccount(account: storage.Account): q.Promise<string> {
    account = storage.clone(account);
    account.id = shortid.generate();

    const emailAddress = Keys.getEmailShortcutAddress(account.email);
    const accountAddress = Keys.getAccountAddress(account.id);

    return this._setupPromise
      .then(async () => {
        await this._dynamoClient.send(new PutItemCommand({
          TableName: AWSStorage.TABLE_NAME,
          Item: marshall(this.wrap(account, emailAddress.partitionKey, emailAddress.sortKey)),
          ConditionExpression: 'attribute_not_exists(partitionKey)'
        }));

        await this._dynamoClient.send(new PutItemCommand({
          TableName: AWSStorage.TABLE_NAME,
          Item: marshall(this.wrap(account, accountAddress.partitionKey, accountAddress.sortKey))
        }));

        return account.id;
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public updateAccount(accountId: string, updateProperties: storage.UpdateAccountProperties): q.Promise<void> {
    return this._setupPromise
      .then(async () => {
        const account = await this.getAccount(accountId);
        const emailAddress = Keys.getEmailShortcutAddress(account.email);

        await this._dynamoClient.send(new UpdateItemCommand({
          TableName: AWSStorage.TABLE_NAME,
          Key: marshall({
            partitionKey: emailAddress.partitionKey,
            sortKey: emailAddress.sortKey
          }),
          UpdateExpression: 'SET azureAdId = :azureAdId, gitHubId = :gitHubId, microsoftId = :microsoftId',
          ExpressionAttributeValues: marshall({
            ':azureAdId': updateProperties.azureAdId,
            ':gitHubId': updateProperties.gitHubId,
            ':microsoftId': updateProperties.microsoftId
          })
        }));
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public removeApp(accountId: string, appId: string): q.Promise<void> {
    return this._setupPromise
      .then(() => this.getApp(accountId, appId))
      .then(async (app) => {
        const partitionKey = Keys.getAppPartitionKey(appId);
        const sortKey = Keys.getHierarchicalAppRowKey(appId);

        await this._dynamoClient.send(new DeleteItemCommand({
          TableName: AWSStorage.TABLE_NAME,
          Key: marshall({ partitionKey, sortKey })
        }));

        // Remove app pointers for all collaborators
        const promises = Object.values(app.collaborators).map(collab =>
          this.removeAppPointer(collab.accountId, appId)
        );

        return q.all(promises);
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public transferApp(fromAccountId: string, toAccountId: string, appId: string): q.Promise<void> {
    return this._setupPromise
      .then(() => {
        const getFromAccountPromise = this.getAccount(fromAccountId);
        const getToAccountPromise = this.getAccount(toAccountId);
        const getAppPromise = this.getApp(fromAccountId, appId, true);
        return q.all([getFromAccountPromise, getToAccountPromise, getAppPromise]);
      })
      .spread((fromAccount: storage.Account, toAccount: storage.Account, app: storage.App) => {
        const fromEmail = fromAccount.email;
        const toEmail = toAccount.email;

        if (!app.collaborators[fromEmail] || !AWSStorage.isOwner(app.collaborators, fromEmail)) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }

        app.collaborators[toEmail] = {
          accountId: toAccountId,
          permission: storage.Permissions.Owner
        };

        app.collaborators[fromEmail] = {
          accountId: fromAccountId,
          permission: storage.Permissions.Collaborator
        };

        return this.updateAppWithPermission(fromAccountId, app, true);
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public getDeploymentIdFromKey(deploymentKey: string): q.Promise<DeploymentKeyPointer> {
    const partitionKey = Keys.getShortcutDeploymentKeyPartitionKey(deploymentKey);
    const sortKey = Keys.getShortcutDeploymentKeyRowKey();

    return this._setupPromise
      .then(async () => {
        const response = await this._dynamoClient.send(new GetItemCommand({
          TableName: AWSStorage.TABLE_NAME,
          Key: marshall({ partitionKey, sortKey })
        }));

        if (!response.Item) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }

        return this.unwrap(response.Item) as DeploymentKeyPointer;
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public getDeployment(accountId: string, appId: string, deploymentId: string): q.Promise<storage.Deployment> {
    return this._setupPromise
      .then(() => {
        return this.retrieveByAppHierarchy(appId, deploymentId);
      })
      .then((flatDeployment: any) => {
        return AWSStorage.unflattenDeployment(flatDeployment);
      })
      .catch(AWSStorage.awsErrorHandler);
  }


  public getDeploymentInfo(deploymentKey: string): q.Promise<storage.DeploymentInfo> {
    return this.getDeploymentIdFromKey(deploymentKey)
      .then((pointer) => {
        return this.getDeployment(pointer.accountId, pointer.appId, pointer.deploymentId)
          .then((deployment) => ({
            id: deployment.id,
            key: deployment.key,
            name: deployment.name,
            package: deployment.package,
            appId: pointer.appId,
            deploymentId: pointer.deploymentId
          }));
      });
  }

  public removeDeployment(accountId: string, appId: string, deploymentId: string): q.Promise<void> {
    return this._setupPromise
      .then(async () => {
        const partitionKey = Keys.getAppPartitionKey(appId);
        const sortKey = Keys.getHierarchicalAppRowKey(appId, deploymentId);

        await this._dynamoClient.send(new DeleteItemCommand({
          TableName: AWSStorage.TABLE_NAME,
          Key: marshall({ partitionKey, sortKey })
        }));
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public updateDeployment(accountId: string, appId: string, deployment: storage.Deployment): q.Promise<void> {
    if (!deployment.id) throw new Error(AWSStorage.NO_ID_ERROR);

    const flatDeployment = AWSStorage.flattenDeployment(deployment);
    return q(this.insertByAppHierarchy(flatDeployment, appId, deployment.id))
      .then(() => {
        return;
      });
  }

  public getPackageHistoryFromDeploymentKey(deploymentKey: string): q.Promise<storage.Package[]> {
    return this.getDeploymentIdFromKey(deploymentKey)
      .then((pointer) => {
        return this.getPackageHistoryFromBlob(pointer.deploymentId);
      });
  }

  public getPackageHistory(accountId: string, appId: string, deploymentId: string): q.Promise<storage.Package[]> {
    return q(this.getPackageHistoryFromBlob(deploymentId));
  }

  public updatePackageHistory(accountId: string, appId: string, deploymentId: string, packages: storage.Package[]): q.Promise<void> {
    return q(this.uploadToHistoryBlob(deploymentId, JSON.stringify(packages)));
  }

  public addBlob(blobId: string, addstream: Readable, streamLength: number): q.Promise<string> {
    return q.Promise<string>((resolve, reject) => {
      const chunks: Buffer[] = [];
      addstream.on('data', chunk => chunks.push(chunk));
      addstream.on('end', () => {
        const content = Buffer.concat(chunks);
        const hash = utils.hashWithSHA256(content.toString());
        this.uploadPackage(hash, content)
          .then(() => resolve(hash))
          .catch(reject);
      });
      addstream.on('error', reject);
    });
  }

  private uploadPackage(packageHash: string, content: Buffer): q.Promise<void> {
    return this._setupPromise
      .then(async () => {
        await this._s3Client.send(new PutObjectCommand({
          Bucket: AWSStorage.TABLE_NAME,
          Key: `packages/${packageHash}`,
          Body: content
        }));
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public getBlob(blobId: string): q.Promise<Buffer> {
    return this.downloadPackage(blobId)
      .then((stream) => {
        return new Promise((resolve, reject) => {
          const chunks: Buffer[] = [];
          stream.on('data', (chunk) => chunks.push(chunk));
          stream.on('end', () => resolve(Buffer.concat(chunks)));
          stream.on('error', reject);
        });
      });
  }

  private downloadPackage(packageHash: string): q.Promise<stream.Readable> {
    return this._setupPromise
      .then(async () => {
        const response = await this._s3Client.send(new GetObjectCommand({
          Bucket: AWSStorage.TABLE_NAME,
          Key: `packages/${packageHash}`
        }));

        return response.Body as stream.Readable;
      })
      .catch(AWSStorage.awsErrorHandler);
  }

  public getBlobUrl(blobId: string): q.Promise<string> {
    throw new Error("Not implemented");
  }

  public removeBlob(blobId: string): q.Promise<void> {
    throw new Error("Not implemented");
  }

  public addAccessKey(accountId: string, accessKey: storage.AccessKey): q.Promise<string> {
    throw new Error("Not implemented");
  }

  public getAccessKey(accountId: string, accessKeyId: string): q.Promise<storage.AccessKey> {
    throw new Error("Not implemented");
  }

  public removeAccessKey(accountId: string, accessKeyId: string): q.Promise<void> {
    throw new Error("Not implemented");
  }

  public getAccessKeys(accountId: string): q.Promise<storage.AccessKey[]> {
    throw new Error("Not implemented");
  }

  public updateAccessKey(accountId: string, accessKey: storage.AccessKey): q.Promise<void> {
    throw new Error("Not implemented");
  }

  public dropAll(): q.Promise<void> {
    throw new Error("Not implemented");
  }
}