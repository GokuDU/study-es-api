package com.guo.utils;

import com.guo.pojo.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Component
public class HtmlParseUtil {
    public static void main(String[] args) throws IOException {
        new HtmlParseUtil().parseJD("死亡搁浅").forEach(System.out::println);
    }

    public List<Content> parseJD(String keywords) throws IOException  {
        // 获取请求 https://search.jd.com/Search?keyword=java
        String url = "https://search.jd.com/Search?keyword=" + keywords + "&enc=utf-8";
        // 解析网页  Jsoup返回的 Document 就是浏览器Document对象
        Document document = Jsoup.parse(new URL(url), 30000);
        // 所有在js的方法在这里都能用
        Element element = document.getElementById("J_goodsList");
        // 获取所有的 li 元素
        Elements liElement = element.getElementsByTag("li");

        List<Content> contentList = new ArrayList<>();

        // 获取 li 元素中的内容
        for (Element el : liElement) {
            // 关于这种图片的网站，所有的图片都是延迟加载的
            // source-data-lazy-img
            String img = el.getElementsByTag("img").eq(0).attr("src");
            String price = el.getElementsByClass("p-price").eq(0).text();
            String title = el.getElementsByClass("p-name").eq(0).text();

            Content content = new Content();
            content.setImg(img);
            content.setPrice(price);
            content.setTitle(title);

            contentList.add(content);
        }
        return contentList;
    }
}
