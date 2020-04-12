package tech.donau.data;

import org.neo4j.driver.types.Node;

public class Book {
    private String title;
    private Integer pages;

    public Book() {
    }

    public Book(String title, Integer pages) {
        this.title = title;
        this.pages = pages;
    }

    public static Book from(Node node) {
        final Book book = new Book();
        book.setTitle(node.get("title").asString());
        book.setPages(node.get("pages").asInt());
        return book;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Integer getPages() {
        return pages;
    }

    public void setPages(Integer pages) {
        this.pages = pages;
    }
}
