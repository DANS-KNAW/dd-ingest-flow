package nl.knaw.dans.ingest.core;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DatasetAuthor {
    private String titles;
    private String initials;
    private String insertions;
    private String surname;
    private String dai;
    private String isni;
    private String orcid;
    private String role;
    private String organization;
}
