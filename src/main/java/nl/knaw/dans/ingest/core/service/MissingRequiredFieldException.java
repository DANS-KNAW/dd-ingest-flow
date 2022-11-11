package nl.knaw.dans.ingest.core.service;

public class MissingRequiredFieldException extends Exception {
    private final String title;

    public MissingRequiredFieldException(String title) {
        super();
        this.title = title;
    }
}
