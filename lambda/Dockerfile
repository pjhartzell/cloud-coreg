FROM continuumio/miniconda3

# Install missing cv2 dependency
RUN apt-get update
RUN apt-get install -y libgl1

# Create and activate the Conda environment
COPY environment.yml .
RUN conda env create -f environment.yml

# Automatically activate the conda environment
RUN echo "source activate cloud-coreg" >> ~/.bashrc
ENV PATH /opt/conda/envs/cloud-coreg/bin:$PATH

# Lambda
RUN pip install awslambdaric
COPY cloud_coreg ./cloud_coreg

ENTRYPOINT [ "python", "-m", "awslambdaric" ]
CMD [ "cloud_coreg.app.handler" ]
