
Number.prototype.padLeft = function(total, char = "0") {
    const str = "" + this;
    return Array( total - str.length + 1 ).join(char) + str;
}

function drawCircle(color) {
    const circle = $("<svg />").attr({
        viewBox: "0 0 200 200",
        xmlns: "http://www.w3.org/2000/svg"
    });
    $("<circle />").attr({
        cx: 100,
        cy: 100,
        r: 90,
        stroke: "black",
        "stroke-width": 10,
        fill: color
    }).appendTo(circle);
    return circle;
}

function getPipelines() {
    return new Promise(function(resolve, reject) {

        // read the list of pipelines
        $.ajax({
            url: "/pipelines",
            json: true,
            cache: false
        }).done(function(pipelines, status, xhr) {

            // gather all outputs because those will drive the next queries
            const datasets = [];

            // build the pipeline interface
            const root = $("#pipelines");
            pipelines.forEach(function(pipeline) {
                const pipeline_div = $("<div />").addClass("pipeline").appendTo(root);

                // pipeline info
                const pipeline_info_div = $("<div />").addClass("flow").appendTo(pipeline_div);
                $("<div />").text(pipeline.name).appendTo(pipeline_info_div);

                // activities
                pipeline.properties.activities.forEach(function(activity, index) {

                    // show the activity icon and name
                    const activity_div = $("<div />").addClass("activity").appendTo(pipeline_div);
                    const activity_icon_div = $("<div />").addClass("icon").appendTo(activity_div);
                    switch(activity.type) {
                        case "Copy":
                            $("<img />").attr({ src: "img/copy.png" }).appendTo(activity_icon_div);
                            break;
                        case "HDInsightPig":
                            $("<img />").attr({ src: "img/pig.png" }).appendTo(activity_icon_div);
                            break;
                        case "HDInsightMapReduce":
                            $("<img />").attr({ src: "img/mapreduce.png" }).appendTo(activity_icon_div);
                            break;
                    }
                    const activity_name_div = $("<div />").addClass("name").text(activity.name).appendTo(activity_div);

                    // success status bubble
                    const activity_success_bubble_div = $("<div />").addClass("bubble success").appendTo(activity_div);
                    drawCircle("green").appendTo(activity_success_bubble_div)
                    activity_success_bubble_div.html( activity_success_bubble_div.html() ); // fix for jQuery not rendering svg
                    const activity_success_counter_div = $("<div />").addClass("counter success").text("??").appendTo(activity_div);

                    // running status bubble
                    const activity_running_bubble_div = $("<div />").addClass("bubble running").appendTo(activity_div);
                    drawCircle("blue").appendTo(activity_running_bubble_div);
                    activity_running_bubble_div.html( activity_running_bubble_div.html() ); // fix for jQuery not rendering svg
                    const activity_running_counter_div = $("<div />").addClass("counter running").text("??").appendTo(activity_div);

                    // failure status bubble
                    const activity_failure_bubble_div = $("<div />").addClass("bubble failure").appendTo(activity_div);
                    drawCircle("red").appendTo(activity_failure_bubble_div);
                    activity_failure_bubble_div.html( activity_failure_bubble_div.html() ); // fix for jQuery not rendering svg
                    const activity_failure_counter_div = $("<div />").addClass("counter failure").text("??").appendTo(activity_div);

                    // find all related outputs
                    activity.outputs.forEach(function(output) {
                        const dataset = datasets.find(function(o) { return o.name == output.name; });
                        if (dataset) {
                            dataset.success.push( activity_success_counter_div );
                            dataset.running.push( activity_running_counter_div );
                            dataset.failure.push( activity_failure_counter_div );
                        } else {
                            datasets.push({
                                name: output.name,
                                success: [ activity_success_counter_div ],
                                running: [ activity_running_counter_div ],
                                failure: [ activity_failure_counter_div ]
                            });
                        }
                    });

                    // make it clickable
                    $(activity_div).click(function() {
                        const dataset_names = activity.outputs.map(function(output) { return output.name });
                        window.open("/logs.html?pipeline=" + pipeline.name + "&activity=" + activity.name + "&datasets=" + dataset_names, "_blank");
                    });

                    // show the arrow going to the next activity
                    if (index < pipeline.properties.activities.length - 1) {
                        const activity_arrow_div = $("<div />").addClass("flow").appendTo(pipeline_div);
                        $("<img />").attr({ src: "img/arrow.png" }).appendTo(activity_arrow_div);
                    }

                });

                // resolve and pass all outputs back
                resolve(datasets);

            });

        }).fail(function(xhr, status, error) {
            reject(error);
        });

    });
}

function getDatasetSlices(dataset) {
    return new Promise(function(resolve, reject) {

        // query to get the slice data
        $.ajax({
            url: "/slices?datasets=" + dataset.name,
            json: true,
            cache: false
        }).done(function(slices, status, xhr) {

            // count success and failures
            let success = 0, running = 0, failure = 0;
            slices.forEach(function(slice) {
                switch(slice.status) {
                    case "Ready":
                        success++;
                        break;
                    case "PendingExecution":
                    case "InProgress":
                        running++;
                        break;
                    case "Failed":
                        failure++;
                        break;
                }
            });

            // increment the success counter
            dataset.success.forEach(function(div) {
                const value = ( isNaN(div.text()) ) ? 0 : parseInt(div.text());
                div.text( (value + success).padLeft(2) );
            });

            // increment the running counter
            dataset.running.forEach(function(div) {
                const value = ( isNaN(div.text()) ) ? 0 : parseInt(div.text());
                div.text( (value + running).padLeft(2) );
            });

            // increment the failure counter
            dataset.failure.forEach(function(div) {
                const value = ( isNaN(div.text()) ) ? 0 : parseInt(div.text());
                div.text( (value + failure).padLeft(2) );
            });

            // resolve
            resolve();

        }).fail(function(xhr, status, error) {
            reject(error);
        });

    });
}

$(document).ready(function() {

    // get all pipelines
    getPipelines().then(function(datasets) {
        
        // create a pool to query all dataset slices
        let index = 0;
        const pool = new PromisePool(function() {
            if (index < datasets.length) {
                const dataset = datasets[index];
                index++;
                return getDatasetSlices(dataset);
            } else {
                return null;
            }
        }, 4); // 4 concurrent

        // start the pool processing only a few at a time
        pool.start().then(function() {

            

        }, function(error) {
            alert(error);
        });

    }, function(error) {
        alert(error);
    });

});