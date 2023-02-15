from setuptools import setup


setup(
    name='cldfbench_reesinkgive',
    py_modules=['cldfbench_reesinkgive'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'reesinkgive=cldfbench_reesinkgive:Dataset',
        ]
    },
    install_requires=[
        'cldfbench[glottolog,excel]',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
