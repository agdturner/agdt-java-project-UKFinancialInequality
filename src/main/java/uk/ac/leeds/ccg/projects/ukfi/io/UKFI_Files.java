/*
 * 
 * Copyright 2018 Andy Turner, The University of Leeds, UK.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package uk.ac.leeds.ccg.projects.ukfi.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import uk.ac.leeds.ccg.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.generic.io.Generic_Defaults;
import uk.ac.leeds.ccg.generic.io.Generic_Files;

/**
 * UKHI_Files
 *
 * @author Andy Turner
 * @version 1.0.0
 */
public class UKFI_Files extends Generic_Files {

    /**
     * @param dataDir
     * @throws java.io.IOException If encountered.
     */
    public UKFI_Files(Path dataDir) throws IOException {
        super(new Generic_Defaults(dataDir));
    }

    public Path getWaASInputDir() throws IOException {
        return Paths.get(getInputDir().toString(), WaAS_Strings.s_WaAS,
                 "UKDA-7215-tab", "tab");
    }
    
    public Path getTIDataFile() throws IOException {
        return Paths.get(getInputDir().toString(), "TransparencyInternational",
                "Selection.csv");
    }

    public Path getGeneratedWaASDir() throws IOException {
        Path d = Paths.get(getGeneratedDir().toString(), WaAS_Strings.s_WaAS);
        Files.createDirectories(d);
        return d;
    }

    public Path getGeneratedWaASSubsetsDir() throws IOException {
        Path d = Paths.get(getGeneratedWaASDir().toString(), "Subsets");
        Files.createDirectories(d);
        return d;
    }

    public Path getEnvDataFile() throws IOException {
        return Paths.get(getGeneratedDir().toString(), "Env.dat");
    }
}
