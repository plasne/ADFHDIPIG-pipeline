
function getParameterByName(name, url) {
    if (!url) url = window.location.href;
    name = name.replace(/[\[\]]/g, "\\$&");
    const regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
        results = regex.exec(url);
    if (!results) return null;
    if (!results[2]) return '';
    return decodeURIComponent(results[2].replace(/\+/g, " "));
}

String.prototype.formatAsDate = function(format = "YYYY-MM-DDTHH:mm:ss.SSSSSSSZ") {
    return moment(this, format).utc().format("M-D-YY H:mm:ss UTC");
}

function isProcessing(state) {
    if (state) {
        $(".dataTables_processing", $("#logs table").closest(".dataTables_wrapper")).show();
    } else {
        $(".dataTables_processing", $("#logs table").closest(".dataTables_wrapper")).hide();
    }
}

function query(table, dataset, start) {
    isProcessing(true);
    $.ajax({
        url: "/slice?dataset=" + dataset + "&start=" + start,
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

function requestLogs(a, id, start) {

    // fetching...
    const parent = $(a).parent();
    parent.empty();
    $("<div>fetching...</div>").appendTo(parent);

    // get the log URLs
    $.ajax({
        url: `/logs?runId=${id}&start=${start}`,
        json: true
    }).done(function(logs, status, xhr) {
        parent.empty();
        logs.forEach(function(log) {
            $("<a />").attr("href", log.url).text(log.name).appendTo(parent);
            parent.append("&nbsp;&nbsp;&nbsp;");
        });
    }).fail(function(xhr, status, error) {
        alert(`fail: ${status}`);
    });

}

function detail(data) {

    // get the fetching container
    const container = $("<div />");
    const outer = $("<div />").addClass("detail").appendTo(container);
    const inner = $("<div />").addClass("detail-items").appendTo(outer);

    // show details
    if (data.errorMessage) field("Error Message", data.errorMessage).appendTo(inner);
    if (data.hasLogs) {
        field("Logs", "<a href='javascript:void(0);' onclick='requestLogs(this, \"" + data.id + "\", " + moment(data.dataSliceStart, "YYYY-MM-DDTHH:mm:ss.SSSSSSSZ").valueOf() + ");'>request logs</a>").appendTo(inner);
    }

    return container.html();
}

function build() {

    // define the table
    const table = $("#logs > table").DataTable({
        columns: [
            {
                title: "Type",
                data: "type"
            },
            {
                title: "Status",
                data: "status",
                width: "60px",
                className: "centered"
            },
            {
                title: "Retry Attempt",
                data: "retryAttempt",
                width: "60px",
                className: "centered"
            },
            {
                title: "% Complete",
                data: "percentComplete",
                width: "60px",
                className: "centered"
            },
            {
                title: "Process Start",
                data: "processingStartTime",
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
                title: "Process End",
                data: "processingEndTime",
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
                title: "Logs?",
                data: "hasLogs",
                width: "60px",
                className: "centered"
            }
        ],
        "order": [[ 4, "desc" ]],
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
    const dataset = getParameterByName("dataset");
    const start = getParameterByName("start");

    // headers
    $("#pipeline-name").text( pipeline );
    $("#activity-name").text( activity );
    $("#dataset-name").text( dataset );
    $("#start-name").text( start.formatAsDate("x") );

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
    query(table, dataset, start);

});