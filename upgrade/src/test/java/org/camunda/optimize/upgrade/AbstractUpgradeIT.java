/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.optimize.upgrade;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.camunda.optimize.dto.optimize.query.MetadataDto;
import org.camunda.optimize.service.es.OptimizeElasticsearchClient;
import org.camunda.optimize.service.es.schema.DefaultIndexMappingCreator;
import org.camunda.optimize.service.es.schema.ElasticSearchSchemaManager;
import org.camunda.optimize.service.es.schema.ElasticsearchMetadataService;
import org.camunda.optimize.service.es.schema.IndexMappingCreator;
import org.camunda.optimize.service.es.schema.IndexSettingsBuilder;
import org.camunda.optimize.service.es.schema.OptimizeIndexNameService;
import org.camunda.optimize.service.es.schema.index.MetadataIndex;
import org.camunda.optimize.service.es.schema.index.index.ImportIndexIndex;
import org.camunda.optimize.service.es.schema.index.index.TimestampBasedImportIndex;
import org.camunda.optimize.service.es.schema.index.report.CombinedReportIndex;
import org.camunda.optimize.service.exceptions.OptimizeRuntimeException;
import org.camunda.optimize.service.util.configuration.ConfigurationService;
import org.camunda.optimize.upgrade.plan.UpgradeExecutionDependencies;
import org.camunda.optimize.upgrade.util.UpgradeUtil;
import org.camunda.optimize.upgrade.version30.AlertIndexV2;
import org.camunda.optimize.upgrade.version30.SingleDecisionReportIndexV2;
import org.camunda.optimize.upgrade.version30.SingleProcessReportIndexV2;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;
import static org.camunda.optimize.service.util.configuration.ConfigurationServiceBuilder.createDefaultConfiguration;
import static org.camunda.optimize.upgrade.EnvironmentConfigUtil.createEmptyEnvConfig;
import static org.camunda.optimize.upgrade.EnvironmentConfigUtil.deleteEnvConfig;

public abstract class AbstractUpgradeIT {

  protected static final MetadataIndex METADATA_INDEX = new MetadataIndex();
  protected static final SingleProcessReportIndexV2 SINGLE_PROCESS_REPORT_INDEX = new SingleProcessReportIndexV2();
  protected static final SingleDecisionReportIndexV2 SINGLE_DECISION_REPORT_INDEX = new SingleDecisionReportIndexV2();
  protected static final CombinedReportIndex COMBINED_REPORT_INDEX = new CombinedReportIndex();
  protected static final TimestampBasedImportIndex TIMESTAMP_BASED_IMPORT_INDEX = new TimestampBasedImportIndex();
  protected static final ImportIndexIndex IMPORT_INDEX_INDEX = new ImportIndexIndex();
  protected static final AlertIndexV2 ALERT_INDEX = new AlertIndexV2();

  private ObjectMapper objectMapper;
  protected OptimizeElasticsearchClient prefixAwareClient;
  protected OptimizeIndexNameService indexNameService;
  protected UpgradeExecutionDependencies upgradeDependencies;
  private ConfigurationService configurationService;
  private ElasticsearchMetadataService metadataService;

  @AfterEach
  public void after() throws Exception {
    cleanAllDataFromElasticsearch();
    deleteEnvConfig();
  }

  @BeforeEach
  protected void setUp() throws Exception {
    configurationService = createDefaultConfiguration();
    if (upgradeDependencies == null) {
      upgradeDependencies = UpgradeUtil.createUpgradeDependencies();
      objectMapper = upgradeDependencies.getObjectMapper();
      prefixAwareClient = upgradeDependencies.getEsClient();
      indexNameService = upgradeDependencies.getIndexNameService();
      metadataService = upgradeDependencies.getMetadataService();
    }

    cleanAllDataFromElasticsearch();
    createEmptyEnvConfig();
  }

  protected void initSchema(List<IndexMappingCreator> mappingCreators) {
    final ElasticSearchSchemaManager elasticSearchSchemaManager = new ElasticSearchSchemaManager(
      metadataService, createDefaultConfiguration(), indexNameService, mappingCreators, objectMapper
    );
    elasticSearchSchemaManager.initializeSchema(prefixAwareClient);
  }

  protected void setMetadataIndexVersion(String version) {
    metadataService.writeMetadata(prefixAwareClient, new MetadataDto(version));
  }

  protected void createOptimizeIndexWithTypeAndVersion(DefaultIndexMappingCreator indexMapping,
                                                       int version) throws IOException {
    final String aliasName = indexNameService.getOptimizeIndexAliasForIndex(indexMapping.getIndexName());
    final String indexName = getVersionedIndexName(indexMapping.getIndexName(), version);
    final Settings indexSettings = createIndexSettings(indexMapping);

    CreateIndexRequest request = new CreateIndexRequest(indexName);
    request.alias(new Alias(aliasName));
    request.settings(indexSettings);
    indexMapping.setDynamic("false");
    prefixAwareClient.getHighLevelClient().indices().create(request, RequestOptions.DEFAULT);
  }

  protected void executeBulk(final String bulkPayload) throws IOException {
    final Request request = new Request(HttpPost.METHOD_NAME, "/_bulk");
    final HttpEntity entity = new NStringEntity(
      UpgradeUtil.readClasspathFileAsString(bulkPayload),
      ContentType.APPLICATION_JSON
    );
    request.setEntity(entity);
    prefixAwareClient.getLowLevelClient().performRequest(request);
    prefixAwareClient.getHighLevelClient().indices().refresh(new RefreshRequest(), RequestOptions.DEFAULT);
  }

  protected String getVersionedIndexName(final String indexName, final int version) {
    return indexNameService.getOptimizeIndexNameForAliasAndVersion(
      indexNameService.getOptimizeIndexAliasForIndex(indexName),
      String.valueOf(version)
    );
  }

  private Settings createIndexSettings(IndexMappingCreator indexMappingCreator) {
    try {
      return IndexSettingsBuilder.buildAllSettings(configurationService, indexMappingCreator);
    } catch (IOException e) {
      throw new OptimizeRuntimeException("Could not create index settings");
    }
  }

  private void cleanAllDataFromElasticsearch() {
    try {
      prefixAwareClient.getHighLevelClient().indices().delete(new DeleteIndexRequest("_all"), RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new RuntimeException("Failed cleaning elasticsearch");
    }
  }

  @SneakyThrows
  protected boolean indexExists(String indexName) {
    return upgradeDependencies.getEsClient()
      .getHighLevelClient()
      .indices()
      .exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
  }

  @SneakyThrows
  protected <T> List<T> getAllDocumentsOfIndexAs(final String indexName, final Class<T> valueType) {
    final SearchHit[] searchHits = getAllDocumentsOfIndex(indexName);
    return Arrays
      .stream(searchHits)
      .map(doc -> {
        try {
          return objectMapper.readValue(
            doc.getSourceAsString(), valueType
          );
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      })
      .collect(toList());
  }

  protected SearchHit[] getAllDocumentsOfIndex(final String... indexNames) throws IOException {
    final SearchResponse searchResponse = prefixAwareClient.search(
      new SearchRequest(indexNames).source(new SearchSourceBuilder().size(10000)),
      RequestOptions.DEFAULT
    );
    return searchResponse.getHits().getHits();
  }

  @SneakyThrows
  protected <T> Optional<T> getDocumentByIdAs(final String indexName, final String id, final Class<T> valueType) {
    final GetResponse getResponse = prefixAwareClient.get(
      new GetRequest(indexName, id), RequestOptions.DEFAULT
    );
    if (getResponse.isExists()) {
      return Optional.ofNullable(objectMapper.readValue(getResponse.getSourceAsString(), valueType));
    } else {
      return Optional.empty();
    }
  }

}
