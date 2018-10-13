/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.atlas.types

import java.io.File
import java.net.{URI, URISyntaxException}
import java.util.{Collections, Date}

import com.google.common.collect.ImmutableSet
import com.hortonworks.spark.atlas.{AtlasClient, AtlasClientConf}

import scala.collection.JavaConverters._
import org.apache.atlas.AtlasConstants
import org.apache.atlas.hbase.bridge.HBaseAtlasHook._
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}
import org.apache.commons.lang.RandomStringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.types.StructType
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

object external extends Logging {
  // External metadata types used to link with external entities

  def objectId(entity: AtlasEntity): AtlasObjectId = {
    val qualifiedName = entity.getAttribute("qualifiedName")
    new AtlasObjectId(entity.getGuid, entity.getTypeName,
      Collections.singletonMap("qualifiedName", qualifiedName))
  }

  // ================ File system entities ======================
  val FS_PATH_TYPE_STRING = "fs_path"
  val HDFS_PATH_TYPE_STRING = "hdfs_path"

  def pathToEntity(path: String)(implicit atlasClient: AtlasClient): AtlasEntity = {
    val uri = resolveURI(path)
    val fsPath = new Path(uri)
    def clusterName: String = atlasClient.conf.get(AtlasClientConf.CLUSTER_NAME)

    val scheme = uri.getScheme

    logWarn(s"Scheme: $scheme")
    val entity = if (uri.getScheme == "hdfs") {
      val entity = new AtlasEntity(HDFS_PATH_TYPE_STRING)
      entity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, uri.getAuthority)
      entity.setAttribute("name",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("path",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("qualifiedName", uri.toString)
      entity
    } else if (uri.getScheme == metadata.S3_SCHEME || uri.getScheme == metadata.S3A_SCHEME) {
      logWarn("Detected S3")

      val strPath = fsPath.toString.toLowerCase
      val bucketName = fsPath.toUri.getAuthority
      val bucketQualifiedName = (fsPath.toUri.getScheme
        + metadata.SCHEME_SEPARATOR
        + fsPath.toUri.getAuthority)
      val pathName = strPath.substring(0, strPath.lastIndexOf('/'))
      val pathQualifiedName = pathName + metadata.QNAME_SEP_CLUSTER_NAME + clusterName

      val objectName = strPath.substring(strPath.lastIndexOf('/') + 1, strPath.length())

      // Yes this is a side effect
      val bucketEntity = new AtlasEntity(metadata.AWS_S3_BUCKET)
      bucketEntity.setAttribute("qualifiedName", bucketQualifiedName)
      bucketEntity.setAttribute("name", bucketName)

      // slight hack :-)
      // val headers = atlasClient.createEntities(Seq(bucketEntity))
      // val guid = headers.getOrElse(bucketEntity.getGuid, bucketEntity.getGuid)
      // bucketEntity.setGuid(guid)

      val folderEntity = new AtlasEntity(metadata.AWS_S3_PSEUDO_DIR)
      folderEntity.setAttribute(metadata.ATTRIBUTE_BUCKET, objectId(bucketEntity))
      folderEntity.setAttribute("qualifiedName", pathQualifiedName)

      folderEntity.setAttribute(metadata.ATTRIBUTE_OBJECT_PREFIX,
        pathName)
      folderEntity.setAttribute("name",
        pathName)

      val headers = atlasClient.createEntities(Seq(bucketEntity, folderEntity))
      val guid = headers.getOrElse(folderEntity.getGuid, folderEntity.getGuid)
      folderEntity.setGuid(guid)

      val entity = new AtlasEntity(metadata.AWS_S3_OBJECT)
      entity.setAttribute(metadata.ATTRIBUTE_FOLDER, objectId(folderEntity))
      entity.setAttribute("qualifiedName", strPath)
      entity.setAttribute("name", objectName)
      entity
    }
    else {
      val entity = new AtlasEntity(FS_PATH_TYPE_STRING)
      entity.setAttribute("name",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("path",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("qualifiedName", uri.toString)
      entity
    }

    entity
  }

  def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (uri.getFragment() != null) {
        val absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI()
        return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
  }

  // ================ HBase entities ======================
  val HBASE_NAMESPACE_STRING = "hbase_namespace"
  val HBASE_TABLE_STRING = "hbase_table"
  val HBASE_COLUMNFAMILY_STRING = "hbase_column_family"
  val HBASE_COLUMN_STRING = "hbase_column"

  def hbaseTableToEntity(cluster: String, tableName: String, nameSpace: String)
      : Seq[AtlasEntity] = {
    val hbaseEntity = new AtlasEntity(HBASE_TABLE_STRING)
    hbaseEntity.setAttribute("qualifiedName",
      getTableQualifiedName(cluster, nameSpace, tableName))
    hbaseEntity.setAttribute("name", tableName.toLowerCase)
    hbaseEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, cluster)
    hbaseEntity.setAttribute("uri", nameSpace.toLowerCase + ":" + tableName.toLowerCase)
    Seq(hbaseEntity)
  }

  // ================ Kafka entities =======================
  val KAFKA_TOPIC_STRING = "kafka_topic"

  def kafkaToEntity(cluster: String, topicName: String): Seq[AtlasEntity] = {
    val kafkaEntity = new AtlasEntity(KAFKA_TOPIC_STRING)
    kafkaEntity.setAttribute("qualifiedName",
      topicName.toLowerCase + '@' + cluster)
    kafkaEntity.setAttribute("name", topicName.toLowerCase)
    kafkaEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, cluster)
    kafkaEntity.setAttribute("uri", topicName.toLowerCase)
    kafkaEntity.setAttribute("topic", topicName.toLowerCase)
    Seq(kafkaEntity)
  }

  // ================== Hive entities =====================
  val HIVE_DB_TYPE_STRING = "hive_db"
  val HIVE_STORAGEDESC_TYPE_STRING = "hive_storagedesc"
  val HIVE_COLUMN_TYPE_STRING = "hive_column"
  val HIVE_TABLE_TYPE_STRING = "hive_table"

  def hiveDbUniqueAttribute(cluster: String, db: String): String = s"${db.toLowerCase}@$cluster"

  def hiveDbToEntities(dbDefinition: CatalogDatabase,
                       cluster: String,
                       owner: String): Seq[AtlasEntity] = {
    val dbEntity = new AtlasEntity(HIVE_DB_TYPE_STRING)

    dbEntity.setAttribute("qualifiedName",
      hiveDbUniqueAttribute(cluster, dbDefinition.name.toLowerCase))
    dbEntity.setAttribute("name", dbDefinition.name.toLowerCase)
    dbEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, cluster)
    dbEntity.setAttribute("description", dbDefinition.description)
    dbEntity.setAttribute("location", dbDefinition.locationUri.toString)
    dbEntity.setAttribute("parameters", dbDefinition.properties.asJava)
    dbEntity.setAttribute("owner", owner)
    Seq(dbEntity)
  }

  def hiveStorageDescUniqueAttribute(
      cluster: String,
      db: String,
      table: String,
      isTempTable: Boolean = false): String = {
    hiveTableUniqueAttribute(cluster, db, table, isTempTable) + "_storage"
  }

  def hiveStorageDescToEntities(
      storageFormat: CatalogStorageFormat,
      cluster: String,
      db: String,
      table: String,
      isTempTable: Boolean = false): Seq[AtlasEntity] = {
    val sdEntity = new AtlasEntity(HIVE_STORAGEDESC_TYPE_STRING)
    sdEntity.setAttribute("qualifiedName",
      hiveStorageDescUniqueAttribute(cluster, db, table, isTempTable))
    storageFormat.inputFormat.foreach(sdEntity.setAttribute("inputFormat", _))
    storageFormat.outputFormat.foreach(sdEntity.setAttribute("outputFormat", _))
    sdEntity.setAttribute("compressed", storageFormat.compressed)
    sdEntity.setAttribute("parameters", storageFormat.properties.asJava)
    storageFormat.serde.foreach(sdEntity.setAttribute("name", _))
    storageFormat.locationUri.foreach { u => sdEntity.setAttribute("location", u.toString) }
    Seq(sdEntity)
  }

  def hiveColumnUniqueAttribute(
      cluster: String,
      db: String,
      table: String,
      column: String,
      isTempTable: Boolean = false): String = {
    val tableName = hiveTableUniqueAttribute(cluster, db, table, isTempTable)
    val parts = tableName.split("@")
    s"${parts(0)}.${column.toLowerCase}@${parts(1)}"
  }

  def hiveSchemaToEntities(
      schema: StructType,
      cluster: String,
      db: String,
      table: String,
      isTempTable: Boolean = false): List[AtlasEntity] = {
    schema.map { struct =>
      val entity = new AtlasEntity(HIVE_COLUMN_TYPE_STRING)

      entity.setAttribute("qualifiedName",
        hiveColumnUniqueAttribute(cluster, db, table, struct.name, isTempTable))
      entity.setAttribute("name", struct.name)
      entity.setAttribute("type", struct.dataType.typeName)
      entity.setAttribute("comment", struct.getComment())
      entity
    }.toList
  }

  def hiveTableUniqueAttribute(
      cluster: String,
      db: String,
      table: String,
      isTemporary: Boolean = false): String = {
    val tableName = if (isTemporary) {
      if (SessionState.get() != null && SessionState.get().getSessionId != null) {
        s"${table}_temp-${SessionState.get().getSessionId}"
      } else {
        s"${table}_temp-${RandomStringUtils.random(10)}"
      }
    } else {
      table
    }

    s"${db.toLowerCase}.${tableName.toLowerCase}@$cluster"
  }

  def hiveTableToEntities(
      tableDefinition: CatalogTable,
      cluster: String,
      mockDbDefinition: Option[CatalogDatabase] = None): Seq[AtlasEntity] = {
    val db = tableDefinition.identifier.database.getOrElse("default")
    val table = tableDefinition.identifier.table
    val dbDefinition = mockDbDefinition.getOrElse(SparkUtils.getExternalCatalog().getDatabase(db))

    val dbEntities = hiveDbToEntities(dbDefinition, cluster, tableDefinition.owner)
    val sdEntities = hiveStorageDescToEntities(
      tableDefinition.storage, cluster, db, table
      /* isTempTable = false  Spark doesn't support temp table */)
    val schemaEntities = hiveSchemaToEntities(
      tableDefinition.schema, cluster, db, table /* , isTempTable = false */)

    val tblEntity = new AtlasEntity(HIVE_TABLE_TYPE_STRING)
    tblEntity.setAttribute("qualifiedName",
      hiveTableUniqueAttribute(cluster, db, table /* , isTemporary = false */))
    tblEntity.setAttribute("name", table)
    tblEntity.setAttribute("owner", tableDefinition.owner)
    tblEntity.setAttribute("createTime", new Date(tableDefinition.createTime))
    tblEntity.setAttribute("lastAccessTime", new Date(tableDefinition.lastAccessTime))
    tableDefinition.comment.foreach(tblEntity.setAttribute("comment", _))
    tblEntity.setAttribute("db", dbEntities.head)
    tblEntity.setAttribute("sd", sdEntities.head)
    tblEntity.setAttribute("parameters", tableDefinition.properties.asJava)
    tableDefinition.viewText.foreach(tblEntity.setAttribute("viewOriginalText", _))
    tblEntity.setAttribute("tableType", tableDefinition.tableType.name)
    tblEntity.setAttribute("columns", schemaEntities.asJava)

    Seq(tblEntity) ++ dbEntities ++ sdEntities ++ schemaEntities
  }
}
