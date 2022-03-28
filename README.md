# etl_pipeline_test_task

The task includes:
    <ol>
            <li>Download and extract the available data</li>
            <li>Merge additional dataframes to main by PIN(Major+Minor)/by Major in the absence of Minor</li>
            <li>Load it into output folder</li>
    </ol>
Configuration file specification:
    <ul>    
        <li>extract: connection params
            <ul>
                <li>scheme</li>
                <li>host</li>
                <li>path: server path to files</li>
            </ul>
        </li>
        <li>
            directories:
            <ul>
                <li>input_folder: contains download archives</li>
                <li>unpacked_folder: contains extracted archive content</li>
                <li>output_folder: contains transformed data</li>
            </ul>
        </li>
        <li>
            files: 
            <ul>
                <li>main_file: the main data set that contains most of the required columns and to which additional data will be attached</li>
                <li>used_archives: list of required archives which will be downloaded from server</li>
                <li>secondary_files: dictionary which consists of key - name of required file with additional data, value - list of required columns from this file</li>
                <li>required_fields: nested dictionary: primary key - the desired field name in the final dataset, secondary key - the real name in the secondary dataframe, value - the file name that contains this column</li>
                <li>encoding: specific encoding for reading main dataframe</li>
            </ul>
        </li>
    </ul>

