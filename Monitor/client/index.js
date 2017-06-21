
$(document).ready(() => {

    // create the datatable
    const instance_logs = $("#instance_logs").DataTable({
        ajax: {
            url: "/logs",
            dataSrc: ""
        },
        processing: true,
        columns: [
            { data: "index", width: "60px" },
            { data: "ts", width: "200px" },
            { data: "level", width: "50px" },
            { data: "msg" }
        ]
    });

    // add selection
    $("#instance_logs").on("click", "tbody tr", function() {
        instance_logs.$("tr.selected").removeClass("selected");
        $(this).addClass("selected");
        const row = instance_logs.row(this).data();
        alert(row.ts);
    });

    // get the specified logs
    $("#get_instance_logs").click(() => {
        instance_logs.ajax.url("/logs?instanceId=" + $("#instanceId").val()).load();
    });

});