package nl.knaw.dans.ingest.core.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class OriginalFilePathMapping {
    // the first entry is the logical (original) name, as provided by the deposit
    // the second entry is the physical path, renamed to solve filesystem limitations
    private final Map<Path, Path> mappings = new HashMap<>();

    public OriginalFilePathMapping(Collection<Mapping> mappings) {
        for (var mapping: mappings) {
            this.mappings.put(mapping.getOriginalPath(), mapping.getPhysicalPath());
        }
    }

    public boolean hasMapping(Path path) {
        return this.mappings.containsKey(path);
    }

    public Path getPhysicalPath(Path path) {
        return this.mappings.getOrDefault(path, path);
    }

    @Data
    @AllArgsConstructor
    public static class Mapping {
        private Path physicalPath;
        private Path originalPath;
    }
}
