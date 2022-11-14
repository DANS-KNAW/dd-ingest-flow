package nl.knaw.dans.ingest.core;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DatasetOrganization {
    private String name;
    private String role;
    private String isni;
    private String viaf;
}
