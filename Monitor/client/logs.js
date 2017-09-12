
function getParameterByName(name, url) {
    if (!url) url = window.location.href;
    name = name.replace(/[\[\]]/g, "\\$&");
    const regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
        results = regex.exec(url);
    if (!results) return null;
    if (!results[2]) return '';
    return decodeURIComponent(results[2].replace(/\+/g, " "));
}

String.prototype.formatAsDate = function() {
    return moment(this, "YYYY-MM-DDTHH:mm:ss.SSSSSSSZ").utc().format("M-D-YY H:mm:ss UTC");
}

function isProcessing(state) {
    if (state) {
        $(".dataTables_processing", $("#logs table").closest(".dataTables_wrapper")).show();
    } else {
        $(".dataTables_processing", $("#logs table").closest(".dataTables_wrapper")).hide();
    }
}

function query(table, datasets) {
    isProcessing(true);
    $.ajax({
        url: "/slices?datasets=" + datasets,
        json: true
    }).done(function(logs, status, xhr) {
        table.clear();
        table.rows.add(logs);
        table.draw();
        isProcessing(false);
    }).fail(function(xhr, status, error) {
        isProcessing(false);
        alert("fail");
    });
}

function field(name, value) {
    const field = $("<div />");
    $("<div />").text(name + ":").appendTo(field);
    $("<div />").addClass("indent").html(value).appendTo(field);
    return field;
}

function viewExec(pipeline, activity, dataset, start) {
    const ts = moment(data.start).valueOf();
    window.open(`/slices.html?pipeline=${pipeline}&activity=${activity}&dataset=${dataset}&start=${ts}`);
}

function detail(data) {

    // get the fetching container
    const container = $("<div />");
    const outer = $("<div />").addClass("detail").appendTo(container);
    const inner = $("<div />").addClass("detail-items").appendTo(outer);
    $("<div />").text("Fetching details, please wait...").appendTo(inner);

    // request the data
    $.ajax({
        url: "/slice?dataset=" + data.dataset + "&start=" + moment(data.start).valueOf(),
        json: true,
        cache: false
    }).done(function(slices, status, xhr) {
        const inner = $("div.detail-items");
        inner.empty();
        if (slices.length > 0) {
            const slice = slices[slices.length - 1];
            
            // show detail fields for the last entry
            field("Type", slice.type).appendTo(inner);
            field("Status", slice.status).appendTo(inner);
            field("Processing Start Time", slice.processingStartTime.formatAsDate()).appendTo(inner);
            field("Processing End Time", slice.processingEndTime.formatAsDate()).appendTo(inner);
            field("% Complete", slice.percentComplete).appendTo(inner);
            field("Retry Attempt", slice.retryAttempt).appendTo(inner);
            if (slice.errorMessage) field("Error Message", slice.errorMessage).appendTo(inner);
            field("Has Logs?", slice.hasLogs).appendTo(inner);
            if (slices.length > 1) {
                field("More", `<a href='javascript:void(0);' onclick='viewExec(\"${slice.pipelineName}\", \"${slice.activityName}\", \"${data.dataset}\", \"${data.start}\");'>view all executions</a>`).appendTo(inner);
            }

        } else {
            $("<div />").text("There are no executions.").appendTo(inner);
        }
    }).fail(function(error) {
        alert(error);
    });

    return container.html();
}

function build() {

    // define the table
    const table = $("#logs > table").DataTable({
        columns: [
            {
                title: "Dataset",
                data: "dataset"
            },
            {
                title: "Status",
                data: "status",
                width: "60px",
                className: "centered"
            },
            {
                title: "State",
                data: "state",
                width: "60px",
                className: "centered"
            },
            {
                title: "Retry Count",
                data: "retryCount",
                width: "20px",
                className: "centered"
            },
            {
                title: "Long Retry Count",
                data: "longRetryCount",
                width: "20px",
                className: "centered"
            },
            {
                title: "Updated",
                data: "statusUpdateTimestamp",
                width: "190px",
                render: function(data, type, row) {
                    if (type === "display") {
                        return data.formatAsDate();
                    } else {
                        return data;
                    }
                }
            },
            {
                title: "Window Start",
                data: "start",
                width: "190px",
                render: function(data, type, row) {
                    if (type === "display") {
                        return data.formatAsDate();
                    } else {
                        return data;
                    }
                }
            },
            {
                title: "Window End",
                data: "end",
                width: "190px",
                render: function(data, type, row) {
                    if (type === "display") {
                        return data.formatAsDate();
                    } else {
                        return data;
                    }
                }
            }
        ],
        "order": [[ 6, "desc" ]],
        processing: true
    });

    // add row selection
    $("#logs table tbody").on("click", "tr", function () {
        if ($(this).attr("role") === "row") {

            // remove selected style and hide the details window
            $(this).siblings("tr.selected").removeClass("selected").each(function(_, tr) {
                const p_row = table.row(tr);
                if (p_row.child.isShown()) {
                    p_row.child.hide();
                }
            });

            // toggle the class and detail state
            $(this).toggleClass("selected");
            const row = table.row(this);
            if (row.child.isShown()) {
                row.child.hide();
            } else {
                if (row.child.length < 1) {
                    const details = detail(row.data());
                    row.child(details);
                }
                row.child.show();
            }

        }
    });

    return table;
}

$(document).ready(function() {

    // get parameters
    const pipeline = getParameterByName("pipeline");
    const activity = getParameterByName("activity");
    const datasets = getParameterByName("datasets");

    // headers
    $("#pipeline-name").text( pipeline );
    $("#activity-name").text( activity );
    $("#datasets-name").text( datasets );

    // build the table
    const table = build();

    // fix Edge bug where the page options aren't shown
    const pageLengthOptions = $("select[name='DataTables_Table_0_length']");
    const countOfPageLengthOptions = $(pageLengthOptions).children().length;
    if (countOfPageLengthOptions < 1) {
        $("<option />").val(10).text(10).appendTo(pageLengthOptions);
        $("<option />").val(25).text(25).appendTo(pageLengthOptions);
        $("<option />").val(50).text(50).appendTo(pageLengthOptions);
        $("<option />").val(100).text(100).appendTo(pageLengthOptions);
    }

    // query
    query(table, datasets);

});