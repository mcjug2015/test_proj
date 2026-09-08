python_tests(
    name="tests",
    dependencies=["src:lib_test", ":loader_res"],
    resolve="py-reqs-dev",
    batch_compatibility_tag="shared_spark",
)

resources(
    name="loader_res",
    sources=[
        "res/nested_total_enroll.zip",
        "res/MDCR ENROLL AB 15-20_CPS_02ENR_2023.xlsx",
    ],
)
