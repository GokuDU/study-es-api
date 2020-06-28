package com.guo.esapi;

import com.alibaba.fastjson.JSON;
import com.guo.pojo.User;
import com.guo.utils.EsConst;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * es 7.6.x    高级客户端测试 API
 */
@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    // ============ 测试索引 ============
    // 测试索引的创建  Request
    @Test
    void testCreateIndex() throws IOException {
        // 1. 创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("gokufriday_index");
        // 2. 客户端执行请求  client.indices() ==》 IndicesClient   请求后获得响应
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    // 测试获取索引，判断其是否存在
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("gokufriday_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println("exists==>" + exists);
    }

    // 测试删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("gokufriday_index");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println("AcknowledgedResponse==>" + delete.isAcknowledged());
    }

    // ============  测试文档  ============
    // 测试添加文档  put /gokufriday_index/_doc/1
    @Test
    void testAddDocument() throws IOException {
        //  创建对象
        User user = new User("gokufriday005", 23);
        // 创建请求
        IndexRequest request = new IndexRequest("gokufriday_index");

        // 规则 put /gokufriday_index/_doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));

        // 放入我们的请求
        request.source(JSON.toJSONString(user), XContentType.JSON);

        // 客户端发送请求，获取响应结果
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.toString());    // 打印 json 字符串
        System.out.println(indexResponse.status());     // 打印命令返回状态
    }

    // 获取文档，判断是否存在  get /gokufriday_index/_doc/1
    @Test
    void testIsExits() throws IOException {
        GetRequest getRequest = new GetRequest("gokufriday_index", "1");
        // 不获取返回的 _source 的上下文 [非必要,默认有设置]
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println("isExists==>" + exists);
    }

    // 获取文档信息
    @Test
    void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("gokufriday_index", "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println("_source==>" + getResponse.getSourceAsString());    // 打印文档信息
        System.out.println(getResponse);    // 返回的内容和命令是一样的
    }

    // 更新文档信息
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("gokufriday_index", "1");
        updateRequest.timeout("1s");

        User user = new User("updateGoku", 16);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.toString());    // 打印 json 字符串
        System.out.println(updateResponse.status());     // 打印命令返回状态

    }


    // 删除文档信息
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("gokufriday_index", "1");
        deleteRequest.timeout("1s");

        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());

    }

    // 批量插入数据
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("gokudu1", 23));
        userList.add(new User("gokudu2", 23));
        userList.add(new User("gokudu3", 23));
        userList.add(new User("gokufriday1", 19));
        userList.add(new User("gokufriday2", 19));
        userList.add(new User("gokufriday3", 19));

        for (int i = 0; i < userList.size(); i++) {
            // 批量更新和批量删除数据，在这里修改对应的请求即可
            // 这里不填充id数据，会默认生成随机id
            bulkRequest.add(
                    new IndexRequest("gokufriday_index")
//                    .id(""+(i+1))
                            .source(JSON.toJSONString(userList.get(i)), XContentType.JSON));
        }

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        System.out.println(bulkResponse.hasFailures());  // 是否执行有失败的
    }

    // 查询
    // searchRequest  搜索请求
    //      new SearchSourceBuilder   构建搜索条件
    //          QueryBuilders.termQuery       精确匹配
    //          QueryBuilders.matchAllQuery()    匹配所有
    //      new HighlightBuilder()    构建高亮
    //
    //	xxxBuilder  对应ElasticSearch的所有命令
    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest(EsConst.ES_INDEX01);
        // 构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        new HighlightBuilder();
        // 查询条件    可以通过使用 QueryBuilders 工具类来实现
        //  QueryBuilders.termQuery       精确匹配
        //  QueryBuilders.matchAllQuery()    匹配所有
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "gokufriday1");
//        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        // termQueryBuilder [精确匹配构建器] 放入 sourceBuilder [搜索条件构建器]
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        // 将 sourceBuilder 放入请求
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        System.out.println("===================================");
        for (SearchHit documentFields : searchResponse.getHits().getHits()) {
            System.out.println(documentFields.getSourceAsMap());
        }
    }
}
