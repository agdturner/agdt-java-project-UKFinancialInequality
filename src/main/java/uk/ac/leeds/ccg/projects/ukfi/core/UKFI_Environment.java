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
package uk.ac.leeds.ccg.projects.ukfi.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import uk.ac.leeds.ccg.data.core.Data_Environment;
import uk.ac.leeds.ccg.generic.core.Generic_Environment;
import uk.ac.leeds.ccg.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.data.waas.data.WaAS_Data;
import uk.ac.leeds.ccg.generic.io.Generic_IO;
import uk.ac.leeds.ccg.generic.memory.Generic_MemoryManager;
import uk.ac.leeds.ccg.projects.ukfi.io.UKFI_Files;

/**
 * UKFI_Environment
 *
 * @author Andy Turner
 * @version 1.0.0
 */
public class UKFI_Environment extends Generic_MemoryManager {

    public transient UKFI_Files files;
    public transient Generic_Environment env;
    public transient WaAS_Environment we;

    public transient static final String EOL = System.getProperty("line.separator");

    /**
     * @param de
     * @throws java.io.IOException If encountered.
     */
    public UKFI_Environment(Data_Environment de) throws Exception {
        files = new UKFI_Files(de.files.getDir());
        this.env = de.env;
        Path dataDir = Paths.get(de.files.getDir().toString(),
                WaAS_Strings.s_WaAS);
        we = new WaAS_Environment(de, dataDir);
        Path f = we.files.getEnvDataFile();
        if (Files.exists(f)) {
            we.data = (WaAS_Data) Generic_IO.readObject(f);
            we.data.env = we.env;
        } else {
            we.data = new WaAS_Data(we);
        }
    }

    /**
     * @return See {@link WaAS_Environment#checkAndMaybeFreeMemory()}.
     * @throws java.io.IOException If encountered.
     */
    @Override
    public boolean checkAndMaybeFreeMemory() throws IOException {
        return we.checkAndMaybeFreeMemory();
    }

    /**
     * @param hoome IFF true then {@link java.lang.OutOfMemoryError} is handled.
     * @return See {@link WaAS_Environment#cacheDataAny()}.
     * @throws java.io.IOException If encountered.
     */
    @Override
    public boolean swapSomeData(boolean hoome) throws IOException {
        return we.swapSomeData(hoome);
    }

    /**
     * @return See {@link WaAS_Environment#cacheDataAny()}.
     * @throws java.io.IOException If encountered.
     */
    @Override
    public boolean swapSomeData() throws IOException {
        return we.swapSomeData();
    }

    /**
     * @return See {@link WaAS_Environment#cacheData()}.
     * @throws java.io.IOException If encountered.
     */
    public boolean clearSomeData() throws IOException {
        return we.data.clearSomeData();
    }

    /**
     * @return See {@link WaAS_Environment#clearAllData()}.
     * @throws java.io.IOException If encountered.
     */
    public int clearAllData() throws IOException {
        return we.clearAllData();
    }
}