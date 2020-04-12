package tech.donau;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.async.AsyncSession;
import tech.donau.data.Book;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.CompletionStage;

@Path("/books")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class BookResource {

    @Inject
    Driver driver;

    @GET
    public CompletionStage<Response> hello() {
        final AsyncSession session = driver.asyncSession();
        return session.runAsync("MATCH (b:Book) RETURN b ORDER BY b.title")
                .thenCompose(cursor -> cursor.listAsync(record -> Book.from(record.get("b").asNode())))
                .thenCompose(books -> session.closeAsync().thenApply(it -> books))
                .thenApply(Response::ok)
                .thenApply(Response.ResponseBuilder::build);
    }

    @POST
    public CompletionStage<Response> addBook(Book book) {
        final AsyncSession session = driver.asyncSession();
        return session.writeTransactionAsync(tx ->
                tx.runAsync("CREATE (b:Book {title: $title, pages: $pages}) RETURN b",
                        Values.parameters("title", book.getTitle(), "pages", book.getPages()))
                .thenCompose(fn -> fn.singleAsync())
        )
                .thenApply(record -> Book.from(record.get("b").asNode()))
                .thenCompose(createdBook -> session.closeAsync().thenApply(it -> createdBook))
                .thenApply(Response::ok)
                .thenApply(Response.ResponseBuilder::build);
    }
}